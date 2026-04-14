using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;

const int GATEWAY_PORT = 5000;
const string SERVER_IP = "127.0.0.1";
const int SERVER_PORT = 6000;
const string GATEWAY_ID = "GW01";
const string CSV_FILE = "sensors.csv";
const string HISTORY_FILE = "gateway_history.txt";
const string SENSOR_PROFILES_RELATIVE_PATH = "..\\Sensor\\profiles";
const string VIDEO_STREAMS_FOLDER = "video_streams";
const int MAX_BATCH_SIZE = 20;
const int BATCH_TIMEOUT_MS = 300000;
const int SENSOR_TIMEOUT_MS = 30000;
const int MONITOR_INTERVAL_MS = 5000;
const int DEFAULT_VIDEO_FRAME_COUNT = 10;
const string STATE_ACTIVE = "ativo";
const string STATE_MAINTENANCE = "manutencao";
const string STATE_DISABLED = "desativado";
const string STATE_INACTIVE = "inativo";
string[] SUPPORTED_DATA_TYPES = ["TEMP", "HUM", "RUIDO", "PM2.5", "PM10", "LUM", "AQ"];

List<string> dataBuffer = new();
Lock dataBufferLock = new();
Lock csvLock = new();
Lock historyLock = new();
Lock pendingFilesLock = new();
SemaphoreSlim batchSendLock = new(1, 1);
ConcurrentDictionary<string, DateTime> lastSeenUtc = new(StringComparer.OrdinalIgnoreCase);
ConcurrentDictionary<string, int> heartbeatCounters = new(StringComparer.OrdinalIgnoreCase);
ConcurrentDictionary<string, string> sensorStatusReasons = new(StringComparer.OrdinalIgnoreCase);
ConcurrentDictionary<string, SensorSession> activeSessions = new(StringComparer.OrdinalIgnoreCase);
ConcurrentDictionary<string, string> manualStateOverrides = new(StringComparer.OrdinalIgnoreCase);

EnsureCsvExists();
LoadPendingBuffer();

TcpListener listener = new(IPAddress.Any, GATEWAY_PORT);
listener.Start();

Console.WriteLine($"[GATEWAY {GATEWAY_ID}] Ativo na porta {GATEWAY_PORT}...");
Console.WriteLine("Comandos disponíveis: 'send', 'status', 'create', 'state' e 'video'.");

_ = Task.Run(BatchTimerLoop);
_ = Task.Run(ManualCommandLoop);
_ = Task.Run(MonitorSensorsLoop);

while (true)
{
    TcpClient sensorClient = await listener.AcceptTcpClientAsync();
    _ = Task.Run(() => HandleSensorAsync(sensorClient));
}

async Task HandleSensorAsync(TcpClient client)
{
    using (client)
    {
        using NetworkStream stream = client.GetStream();
        using StreamReader reader = new(stream, Encoding.UTF8);
        using StreamWriter writer = new(stream, Encoding.UTF8) { AutoFlush = true };

        string? currentSensorId = null;
        bool isConnected = false;
        HashSet<string> currentTipos = new(StringComparer.OrdinalIgnoreCase);

        try
        {
            while (true)
            {
                string? line = await reader.ReadLineAsync();
                if (line == null)
                {
                    if (currentSensorId != null)
                    {
                        MarkSensorAsInactive(currentSensorId);
                        activeSessions.TryRemove(currentSensorId, out _);
                    }
                    break;
                }

                string[] parts = line.Split('|');
                if (parts.Length == 0)
                {
                    await writer.WriteLineAsync("ERROR|MENSAGEM_INVALIDA");
                    continue;
                }

                string command = parts[0].ToUpperInvariant();

                switch (command)
                {
                    case "CONNECT":
                        if (parts.Length < 4)
                        {
                            await writer.WriteLineAsync("CONN_ACK|ERRO|FORMATO_INVALIDO");
                            break;
                        }

                        string id = parts[1].Trim();
                        string tiposSolicitadosRaw = parts[2];
                        string zona = parts[3].Trim();
                        SensorInfo? info = FindSensor(id);
                        string? connectValidationError = ValidateSensorConnection(info, zona);
                        if (connectValidationError != null)
                        {
                            await writer.WriteLineAsync(connectValidationError);
                            break;
                        }

                        SensorInfo validatedInfo = info!;

                        List<string> tiposSolicitados = tiposSolicitadosRaw
                            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                            .ToList();

                        if (tiposSolicitados.Count == 0)
                        {
                            await writer.WriteLineAsync("CONN_ACK|RECUSADO|SEM_TIPOS");
                            break;
                        }

                        bool todosOsTiposSaoValidos = tiposSolicitados.All(ts =>
                            validatedInfo.TiposDados.Any(csvT => csvT.Equals(ts, StringComparison.OrdinalIgnoreCase)));

                        if (!todosOsTiposSaoValidos)
                        {
                            await writer.WriteLineAsync("CONN_ACK|RECUSADO|TIPO_NAO_AUTORIZADO");
                            Console.WriteLine($"[RECUSADO] Sensor {id} tentou tipos não autorizados: {tiposSolicitadosRaw}");
                            break;
                        }

                        currentSensorId = id;
                        currentTipos = new HashSet<string>(tiposSolicitados, StringComparer.OrdinalIgnoreCase);
                        isConnected = true;
                        MarkSensorAsSeen(id, validatedInfo.Estado == STATE_MAINTENANCE ? "Manutenção em curso." : "Ligação aceite.");
                        string connectedState = GetConnectedState(validatedInfo.Estado);
                        UpdateSensorEntry(id, connectedState, DateTime.UtcNow);

                        string response = validatedInfo.Estado == STATE_MAINTENANCE
                            ? "CONN_ACK|MANUTENCAO"
                            : "CONN_ACK|ACEITE";

                        activeSessions[id] = new SensorSession(id, writer);
                        await writer.WriteLineAsync(response);
                        Console.WriteLine($"[ACEITE] Sensor {id} conectado na zona {zona}. Tipos: {string.Join(", ", currentTipos)}");
                        break;

                    case "DATA":
                        if (!isConnected || currentSensorId == null)
                        {
                            await writer.WriteLineAsync("DATA_ACK|ERRO|NAO_LIGADO");
                            break;
                        }

                        if (parts.Length < 5)
                        {
                            await writer.WriteLineAsync("DATA_ACK|ERRO|FORMATO_INVALIDO");
                            break;
                        }

                        string sensorId = parts[1].Trim();
                        string timestamp = parts[2].Trim();
                        string tipoDado = parts[3].Trim();
                        string valor = parts[4].Trim();

                        if (!sensorId.Equals(currentSensorId, StringComparison.OrdinalIgnoreCase))
                        {
                            await writer.WriteLineAsync("DATA_ACK|ERRO|SENSOR_INVALIDO");
                            break;
                        }

                        if (!currentTipos.Contains(tipoDado))
                        {
                            await writer.WriteLineAsync("DATA_ACK|ERRO|TIPO_NAO_SUPORTADO");
                            break;
                        }

                        if (string.IsNullOrWhiteSpace(valor))
                        {
                            await writer.WriteLineAsync("DATA_ACK|ERRO|VALOR_INVALIDO");
                            break;
                        }

                        string normalizedData = $"DATA|{sensorId}|{timestamp}|{tipoDado.ToUpperInvariant()}|{valor}";

                        LogDataLocally(normalizedData);
                        QueueData(normalizedData);
                        RegisterSensorSeen(currentSensorId);
                        ApplyAutomaticStateUpdate(currentSensorId, STATE_ACTIVE, DateTime.UtcNow);

                        Console.WriteLine($"[RECEBIDO] Sensor {currentSensorId} enviou {tipoDado}: {valor}");
                        await writer.WriteLineAsync("DATA_ACK|SUCESSO");
                        break;

                    case "HEARTBEAT":
                        if (!isConnected || currentSensorId == null)
                        {
                            await writer.WriteLineAsync("ACK_HEARTBEAT|ERRO|NAO_LIGADO");
                            break;
                        }

                        MarkSensorAsSeen(currentSensorId, "Heartbeat recebido.");
                        ApplyAutomaticStateUpdate(currentSensorId, STATE_ACTIVE, DateTime.UtcNow);
                        int heartbeatCount = heartbeatCounters.AddOrUpdate(currentSensorId, 1, (_, current) => current + 1);
                        if (heartbeatCount % 10 == 0)
                        {
                            Console.WriteLine($"[HB] {currentSensorId} confirmou {heartbeatCount} heartbeats.");
                        }
                        await writer.WriteLineAsync("ACK_HEARTBEAT|SUCESSO");
                        break;

                    case "NOTIFY":
                        if (!isConnected || currentSensorId == null)
                        {
                            await writer.WriteLineAsync("ACK_NOTIFY|ERRO|NAO_LIGADO");
                            break;
                        }

                        if (parts.Length < 4)
                        {
                            await writer.WriteLineAsync("ACK_NOTIFY|ERRO|FORMATO_INVALIDO");
                            break;
                        }

                        string notificationType = parts[2].Trim().ToUpperInvariant();
                        string notificationPayload = parts[3].Trim();

                        MarkSensorAsSeen(currentSensorId, $"Notificação {notificationType} recebida.");

                        if (notificationType == "LOW_BATTERY")
                        {
                            Console.WriteLine($"[ALERTA] Bateria baixa no sensor {currentSensorId}: {notificationPayload}");
                            sensorStatusReasons[currentSensorId] = $"Bateria baixa ({notificationPayload}).";
                            ApplyAutomaticStateUpdate(currentSensorId, STATE_MAINTENANCE, DateTime.UtcNow);
                            await writer.WriteLineAsync("ACK_NOTIFY|SUCESSO|MANUTENCAO");
                        }
                        else if (notificationType == "CHARGING" &&
                                 notificationPayload.Equals("INICIADO", StringComparison.OrdinalIgnoreCase))
                        {
                            Console.WriteLine($"[CARREGAMENTO] Sensor {currentSensorId} entrou em carregamento.");
                            sensorStatusReasons[currentSensorId] = "Sensor em carregamento.";
                            ApplyAutomaticStateUpdate(currentSensorId, STATE_MAINTENANCE, DateTime.UtcNow);
                            await writer.WriteLineAsync("ACK_NOTIFY|SUCESSO|A_CARREGAR");
                        }
                        else if (notificationType == "CHARGING" &&
                                 notificationPayload.Equals("COMPLETO", StringComparison.OrdinalIgnoreCase))
                        {
                            Console.WriteLine($"[CARREGAMENTO] Sensor {currentSensorId} terminou o carregamento.");
                            sensorStatusReasons[currentSensorId] = "Carregamento concluído.";
                            ApplyAutomaticStateUpdate(currentSensorId, STATE_ACTIVE, DateTime.UtcNow);
                            await writer.WriteLineAsync("ACK_NOTIFY|SUCESSO|ATIVO");
                        }
                        else
                        {
                            await writer.WriteLineAsync("ACK_NOTIFY|SUCESSO|IGNORADO");
                        }
                        break;

                    case "VIDEO_ACK":
                        if (!isConnected || currentSensorId == null)
                        {
                            break;
                        }

                        if (activeSessions.TryGetValue(currentSensorId, out SensorSession? videoSession))
                        {
                            string payload = parts.Length >= 3
                                ? string.Join('|', parts.Skip(2))
                                : "ERRO|SEM_PAYLOAD";
                            videoSession.TryCompleteVideoAck(payload);
                        }
                        break;

                    case "DISCONNECT":
                        if (!isConnected || currentSensorId == null)
                        {
                            await writer.WriteLineAsync("ACK_DISCONNECT|ERRO|NAO_LIGADO");
                            break;
                        }

                        UpdateSensorEntry(currentSensorId, STATE_INACTIVE, DateTime.UtcNow);
                        lastSeenUtc.TryRemove(currentSensorId, out _);
                        heartbeatCounters.TryRemove(currentSensorId, out _);
                        activeSessions.TryRemove(currentSensorId, out _);
                        sensorStatusReasons[currentSensorId] = "Ligação terminada pelo sensor.";
                        Console.WriteLine($"[DESLIGAR] Sensor {currentSensorId} terminou a ligação.");
                        await writer.WriteLineAsync("ACK_DISCONNECT|SUCESSO");
                        return;

                    default:
                        await writer.WriteLineAsync("ERROR|COMANDO_DESCONHECIDO");
                        break;
                }
            }
        }
        catch (Exception ex)
        {
            if (currentSensorId != null)
            {
                MarkSensorAsInactive(currentSensorId);
                activeSessions.TryRemove(currentSensorId, out _);
            }

            Console.WriteLine($"[GATEWAY] Erro na ligação: {ex.Message}");
        }
    }
}

void QueueData(string data)
{
    bool shouldSend;
    string tipoDado = ExtractDataType(data);

    lock (dataBufferLock)
    {
        dataBuffer.Add(data);
        SavePendingMeasurement(tipoDado, data);
        shouldSend = dataBuffer.Count >= MAX_BATCH_SIZE;
    }

    if (shouldSend)
    {
        _ = Task.Run(ForwardBatchToServerAsync);
    }
}

async Task ForwardBatchToServerAsync()
{
    await batchSendLock.WaitAsync();
    try
    {
        List<string> batch;

        lock (dataBufferLock)
        {
            if (dataBuffer.Count == 0)
            {
                return;
            }

            int countToSend = Math.Min(MAX_BATCH_SIZE, dataBuffer.Count);
            batch = dataBuffer.Take(countToSend).ToList();
        }

        try
        {
            using TcpClient server = new();
            await server.ConnectAsync(SERVER_IP, SERVER_PORT);

            using NetworkStream stream = server.GetStream();
            using StreamWriter sw = new(stream, Encoding.UTF8) { AutoFlush = true };
            using StreamReader sr = new(stream, Encoding.UTF8);

            await sw.WriteLineAsync($"DATA_BATCH|{GATEWAY_ID}|{batch.Count}");
            foreach (string measurement in batch)
            {
                await sw.WriteLineAsync(measurement);
            }
            await sw.WriteLineAsync("END");

            string? ack = await sr.ReadLineAsync();

            if (ack != "BATCH_ACK|SUCESSO")
            {
                Console.WriteLine($"[ERRO] ACK inesperado do servidor: {ack ?? "<null>"}");
                return;
            }

            MarkBatchAsDelivered(batch.Count);
            Console.WriteLine($"[BATCH] Lote de {batch.Count} medições enviado com sucesso.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ERRO] Falha no envio para o servidor: {ex.Message}");
        }
    }
    finally
    {
        batchSendLock.Release();
    }
}

void MarkBatchAsDelivered(int deliveredCount)
{
    lock (dataBufferLock)
    {
        if (deliveredCount <= 0)
        {
            return;
        }

        int countToRemove = Math.Min(deliveredCount, dataBuffer.Count);
        List<string> deliveredItems = dataBuffer.Take(countToRemove).ToList();
        dataBuffer.RemoveRange(0, countToRemove);

        foreach (string deliveredItem in deliveredItems)
        {
            RemovePendingMeasurement(ExtractDataType(deliveredItem), deliveredItem);
        }
    }
}

async Task BatchTimerLoop()
{
    while (true)
    {
        await Task.Delay(BATCH_TIMEOUT_MS);
        await ForwardBatchToServerAsync();
    }
}

async Task ManualCommandLoop()
{
    while (true)
    {
        string? cmd = Console.ReadLine()?.Trim().ToLowerInvariant();

        if (cmd == "send")
        {
            Console.WriteLine("[MANUAL] A forçar envio do lote atual...");
            await ForwardBatchToServerAsync();
        }
        else if (cmd == "status")
        {
            foreach (SensorInfo sensor in ReadSensors())
            {
                Console.WriteLine($"[STATUS] {sensor.SensorId} | {sensor.Estado} | {sensor.Zona} | {sensor.LastSync}");
            }
        }
        else if (cmd == "create")
        {
            CreateSensorInteractive();
        }
        else if (cmd == "state")
        {
            ChangeSensorStateInteractive();
        }
        else if (cmd == "video")
        {
            await RequestVideoInteractiveAsync();
        }
    }
}

async Task RequestVideoInteractiveAsync()
{
    Console.Write("ID do sensor para vídeo: ");
    string sensorId = (Console.ReadLine() ?? "").Trim().ToUpperInvariant();

    if (string.IsNullOrWhiteSpace(sensorId))
    {
        Console.WriteLine("[VIDEO] ID inválido.");
        return;
    }

    SensorInfo? sensor = FindSensor(sensorId);
    if (sensor == null)
    {
        Console.WriteLine($"[VIDEO] Sensor {sensorId} não encontrado.");
        return;
    }

    if (sensor.Estado != STATE_ACTIVE)
    {
        Console.WriteLine($"[VIDEO] O sensor {sensorId} não está ativo.");
        return;
    }

    if (!activeSessions.TryGetValue(sensorId, out SensorSession? session))
    {
        Console.WriteLine($"[VIDEO] O sensor {sensorId} não está ligado ao gateway.");
        return;
    }

    Console.Write($"Número de frames simulados (Enter para {DEFAULT_VIDEO_FRAME_COUNT}): ");
    string? input = Console.ReadLine()?.Trim();
    int frameCount = DEFAULT_VIDEO_FRAME_COUNT;
    if (!string.IsNullOrWhiteSpace(input) && (!int.TryParse(input, out frameCount) || frameCount <= 0))
    {
        Console.WriteLine("[VIDEO] Número de frames inválido.");
        return;
    }

    Directory.CreateDirectory(Path.Combine(Environment.CurrentDirectory, VIDEO_STREAMS_FOLDER));

    using TcpListener videoListener = new(IPAddress.Any, 0);
    videoListener.Start();

    int videoPort = ((IPEndPoint)videoListener.LocalEndpoint).Port;
    TaskCompletionSource<string> videoAck = session.CreatePendingVideoAck();

    try
    {
        await session.SendAsync($"VIDEO_REQ|{sensorId}|{videoPort}|{frameCount}");
        string ackPayload = await videoAck.Task.WaitAsync(TimeSpan.FromSeconds(10));

        if (!ackPayload.StartsWith("ACEITE|", StringComparison.OrdinalIgnoreCase))
        {
            Console.WriteLine($"[VIDEO] Pedido recusado pelo sensor {sensorId}: {ackPayload}");
            return;
        }

        Console.WriteLine($"[VIDEO] Sensor {sensorId} aceitou o pedido. A receber stream na porta {videoPort}...");
        using TcpClient videoClient = await videoListener.AcceptTcpClientAsync().WaitAsync(TimeSpan.FromSeconds(15));
        string savedPath = await ReceiveVideoStreamAsync(sensorId, videoClient);
        Console.WriteLine($"[VIDEO] Stream guardada em {savedPath}");
    }
    catch (TimeoutException)
    {
        Console.WriteLine($"[VIDEO] Timeout à espera da resposta ou da stream do sensor {sensorId}.");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[VIDEO] Erro na sessão de vídeo: {ex.Message}");
    }
    finally
    {
        session.ClearPendingVideoAck(videoAck);
    }
}

async Task<string> ReceiveVideoStreamAsync(string sensorId, TcpClient videoClient)
{
    using NetworkStream stream = videoClient.GetStream();
    using StreamReader reader = new(stream, Encoding.UTF8);

    string sensorDirectory = Path.Combine(Environment.CurrentDirectory, VIDEO_STREAMS_FOLDER, sensorId);
    Directory.CreateDirectory(sensorDirectory);

    string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
    string filePath = Path.Combine(sensorDirectory, $"{timestamp}.txt");
    List<string> lines = new();
    int frameCounter = 0;

    while (true)
    {
        string? line = await reader.ReadLineAsync();
        if (line == null)
        {
            break;
        }

        lines.Add(line);
        if (line.StartsWith("VIDEO_FRAME|", StringComparison.OrdinalIgnoreCase))
        {
            frameCounter++;
        }

        if (line.StartsWith("VIDEO_END|", StringComparison.OrdinalIgnoreCase))
        {
            break;
        }
    }

    File.WriteAllLines(filePath, lines);
    LogDataLocally($"VIDEO|{sensorId}|{timestamp}|{frameCounter}_frames|{Path.GetFileName(filePath)}");
    return filePath;
}

async Task MonitorSensorsLoop()
{
    while (true)
    {
        await Task.Delay(MONITOR_INTERVAL_MS);

        DateTime nowUtc = DateTime.UtcNow;
        List<string> staleSensors = new();

        foreach (KeyValuePair<string, DateTime> entry in lastSeenUtc)
        {
            if (nowUtc - entry.Value > TimeSpan.FromMilliseconds(SENSOR_TIMEOUT_MS))
            {
                staleSensors.Add(entry.Key);
            }
        }

        foreach (string sensorId in staleSensors)
        {
            if (lastSeenUtc.TryRemove(sensorId, out _))
            {
                SensorInfo? sensorInfo = FindSensor(sensorId);
                if (sensorInfo?.Estado == STATE_MAINTENANCE)
                {
                    RegisterSensorSeen(sensorId);
                    if (sensorStatusReasons.TryGetValue(sensorId, out string? reason))
                    {
                        Console.WriteLine($"[MANUTENCAO] Sensor {sensorId} permanece em manutenção. Motivo: {reason}");
                    }
                    continue;
                }

                heartbeatCounters.TryRemove(sensorId, out _);
                sensorStatusReasons[sensorId] = "Ausência de heartbeat.";
                UpdateSensorEntry(sensorId, STATE_INACTIVE, nowUtc);
                Console.WriteLine($"[TIMEOUT] Sensor {sensorId} marcado como inativo por ausência de heartbeat.");
            }
        }
    }
}

void RegisterSensorSeen(string sensorId)
{
    lastSeenUtc[sensorId] = DateTime.UtcNow;
}

void MarkSensorAsInactive(string sensorId)
{
    lastSeenUtc.TryRemove(sensorId, out _);
    heartbeatCounters.TryRemove(sensorId, out _);
    sensorStatusReasons[sensorId] = "Ligação perdida.";
    UpdateSensorEntry(sensorId, STATE_INACTIVE, DateTime.UtcNow);
}

string? ValidateSensorConnection(SensorInfo? info, string zona)
{
    if (info == null)
    {
        return "CONN_ACK|RECUSADO|ID_DESCONHECIDO";
    }

    if (info.Estado == STATE_DISABLED)
    {
        return "CONN_ACK|RECUSADO|SENSOR_DESATIVADO";
    }

    if (!info.Zona.Equals(zona, StringComparison.OrdinalIgnoreCase))
    {
        return "CONN_ACK|RECUSADO|ZONA_INVALIDA";
    }

    return null;
}

void MarkSensorAsSeen(string sensorId, string reason)
{
    RegisterSensorSeen(sensorId);
    sensorStatusReasons[sensorId] = reason;
}

string GetConnectedState(string currentState)
{
    return currentState == STATE_MAINTENANCE
        ? STATE_MAINTENANCE
        : STATE_ACTIVE;
}

void ApplyAutomaticStateUpdate(string sensorId, string automaticState, DateTime timestampUtc)
{
    if (TryGetLockedManualState(sensorId, out string? manualState))
    {
        UpdateSensorEntry(sensorId, manualState!, timestampUtc);
        return;
    }

    UpdateSensorEntry(sensorId, automaticState, timestampUtc);
}

bool TryGetLockedManualState(string sensorId, out string? manualState)
{
    if (manualStateOverrides.TryGetValue(sensorId, out string? overrideState) &&
        (overrideState == STATE_MAINTENANCE || overrideState == STATE_DISABLED))
    {
        manualState = overrideState;
        return true;
    }

    manualState = null;
    return false;
}

bool CanManuallySetActive(string sensorId, out string? reason)
{
    if (!sensorStatusReasons.TryGetValue(sensorId, out string? statusReason))
    {
        reason = null;
        return true;
    }

    if (statusReason.Contains("Bateria baixa", StringComparison.OrdinalIgnoreCase))
    {
        reason = "o sensor continua sinalizado com bateria baixa";
        return false;
    }

    if (statusReason.Contains("carregamento", StringComparison.OrdinalIgnoreCase))
    {
        reason = "o sensor continua em carregamento";
        return false;
    }

    reason = null;
    return true;
}

void UpdateSensorEntry(string id, string estado, DateTime timestampUtc)
{
    lock (csvLock)
    {
        List<string> lines = File.ReadAllLines(CSV_FILE).ToList();

        for (int i = 1; i < lines.Count; i++)
        {
            string[] p = lines[i].Split(';');
            if (p.Length < 5)
            {
                continue;
            }

            if (!p[0].Trim().Equals(id, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            p[1] = estado;
            p[4] = timestampUtc.ToLocalTime().ToString("yyyy-MM-ddTHH:mm:ss");
            lines[i] = string.Join(';', p);
            break;
        }

        File.WriteAllLines(CSV_FILE, lines);
    }
}

void LogDataLocally(string data)
{
    lock (historyLock)
    {
        File.AppendAllText(HISTORY_FILE, $"[{DateTime.Now:yyyy-MM-ddTHH:mm:ss}] {data}{Environment.NewLine}");
    }
}

void LoadPendingBuffer()
{
    lock (dataBufferLock)
    {
        dataBuffer = Directory.GetFiles(Environment.CurrentDirectory, "pending_*.txt")
            .OrderBy(path => path, StringComparer.OrdinalIgnoreCase)
            .SelectMany(path => File.ReadAllLines(path)
                .Where(line => !string.IsNullOrWhiteSpace(line)))
            .ToList();
    }
}

void SavePendingMeasurement(string tipoDado, string data)
{
    lock (pendingFilesLock)
    {
        File.AppendAllText(GetPendingFilePath(tipoDado), data + Environment.NewLine);
    }
}

void RemovePendingMeasurement(string tipoDado, string data)
{
    lock (pendingFilesLock)
    {
        string path = GetPendingFilePath(tipoDado);
        if (!File.Exists(path))
        {
            return;
        }

        List<string> remainingLines = File.ReadAllLines(path).ToList();
        int index = remainingLines.FindIndex(line => line.Equals(data, StringComparison.Ordinal));
        if (index >= 0)
        {
            remainingLines.RemoveAt(index);
        }

        if (remainingLines.Count == 0)
        {
            File.Delete(path);
            return;
        }

        File.WriteAllLines(path, remainingLines);
    }
}

string GetPendingFilePath(string tipoDado)
{
    string safeType = tipoDado.ToUpperInvariant().Replace('.', '_');
    return Path.Combine(Environment.CurrentDirectory, $"pending_{safeType}.txt");
}

string ExtractDataType(string data)
{
    string[] parts = data.Split('|');
    return parts.Length >= 4 ? parts[3].Trim() : "UNKNOWN";
}

SensorInfo? FindSensor(string sensorId)
{
    return ReadSensors().FirstOrDefault(sensor =>
        sensor.SensorId.Equals(sensorId, StringComparison.OrdinalIgnoreCase));
}

List<SensorInfo> ReadSensors()
{
    lock (csvLock)
    {
        if (!File.Exists(CSV_FILE))
        {
            return new List<SensorInfo>();
        }

        return File.ReadAllLines(CSV_FILE)
            .Skip(1)
            .Select(ParseSensorInfo)
            .Where(sensor => sensor != null)
            .Cast<SensorInfo>()
            .ToList();
    }
}

SensorInfo? ParseSensorInfo(string line)
{
    string[] p = line.Split(';');
    if (p.Length < 5)
    {
        return null;
    }

    return new SensorInfo
    {
        SensorId = p[0].Trim(),
        Estado = p[1].Trim().ToLowerInvariant(),
        Zona = p[2].Trim(),
        TiposDados = p[3]
            .Trim().Trim('"')
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(tipo => tipo.Trim().Trim('"'))
            .ToList(),
        LastSync = p[4].Trim()
    };
}

void EnsureCsvExists()
{
    if (File.Exists(CSV_FILE))
    {
        return;
    }

    File.WriteAllLines(CSV_FILE, new[]
    {
        "sensor_id;estado;zona;tipos_dados;last_sync",
        "S101;ativo;ZONA_PARQUE;TEMP,HUM,RUIDO;-",
        "S102;ativo;ZONA_ESCOLAR;TEMP,PM2.5,RUIDO;-",
        "S103;desativado;ZONA_CENTRO;TEMP,HUM;-",
        "S104;ativo;ZONA_PASSEIO;TEMP,HUM,RUIDO;-",
        "S105;ativo;ZONA_ESCOLAR;TEMP,HUM,RUIDO;-"
    });
}

void CreateSensorInteractive()
{
    Console.Write("Novo ID do sensor: ");
    string sensorId = (Console.ReadLine() ?? "").Trim().ToUpperInvariant();

    if (string.IsNullOrWhiteSpace(sensorId))
    {
        Console.WriteLine("[CREATE] ID inválido.");
        return;
    }

    if (FindSensor(sensorId) != null)
    {
        Console.WriteLine($"[CREATE] Já existe um sensor com o ID {sensorId}.");
        return;
    }

    Console.Write("Zona: ");
    string zona = (Console.ReadLine() ?? "").Trim().ToUpperInvariant();
    if (string.IsNullOrWhiteSpace(zona))
    {
        Console.WriteLine("[CREATE] Zona inválida.");
        return;
    }

    Console.WriteLine($"Tipos suportados pelo protocolo: {string.Join(", ", SUPPORTED_DATA_TYPES)}");
    Console.Write("Tipos de dados do sensor (separados por vírgula): ");
    string tiposInput = (Console.ReadLine() ?? "").Trim().ToUpperInvariant();
    List<string> tipos = tiposInput
        .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
        .Select(tipo => tipo.ToUpperInvariant())
        .Distinct(StringComparer.OrdinalIgnoreCase)
        .ToList();

    if (tipos.Count == 0)
    {
        Console.WriteLine("[CREATE] Tens de indicar pelo menos um tipo de dado.");
        return;
    }

    List<string> invalidTypes = tipos
        .Where(tipo => !SUPPORTED_DATA_TYPES.Contains(tipo, StringComparer.OrdinalIgnoreCase))
        .ToList();

    if (invalidTypes.Count > 0)
    {
        Console.WriteLine($"[CREATE] Tipos inválidos: {string.Join(", ", invalidTypes)}");
        Console.WriteLine($"[CREATE] Tipos permitidos: {string.Join(", ", SUPPORTED_DATA_TYPES)}");
        return;
    }

    Console.Write("Estado inicial [ativo/manutencao/desativado] (Enter para 'ativo'): ");
    string estadoInput = (Console.ReadLine() ?? "").Trim().ToLowerInvariant();
    string estado = string.IsNullOrWhiteSpace(estadoInput) ? STATE_ACTIVE : estadoInput;

    if (estado != STATE_ACTIVE && estado != STATE_MAINTENANCE && estado != STATE_DISABLED)
    {
        Console.WriteLine("[CREATE] Estado inválido.");
        return;
    }

    AddSensorToCsv(sensorId, estado, zona, tipos);
    CreateSensorProfile(sensorId, zona, tipos);

    Console.WriteLine($"[CREATE] Sensor {sensorId} criado com sucesso.");
}

void AddSensorToCsv(string sensorId, string estado, string zona, List<string> tipos)
{
    lock (csvLock)
    {
        string line = string.Join(';', new[]
        {
            sensorId,
            estado,
            zona,
            string.Join(',', tipos),
            "-"
        });

        File.AppendAllLines(CSV_FILE, new[] { line });
    }
}

void ChangeSensorStateInteractive()
{
    Console.Write("ID do sensor a alterar: ");
    string sensorId = (Console.ReadLine() ?? "").Trim().ToUpperInvariant();

    SensorInfo? sensor = FindSensor(sensorId);
    if (sensor == null)
    {
        Console.WriteLine($"[STATE] Sensor {sensorId} não encontrado.");
        return;
    }

    Console.WriteLine($"[STATE] Estado atual de {sensorId}: {sensor.Estado}");
    Console.Write("Novo estado [ativo/manutencao/desativado/inativo]: ");
    string newState = (Console.ReadLine() ?? "").Trim().ToLowerInvariant();

    if (newState != STATE_ACTIVE &&
        newState != STATE_MAINTENANCE &&
        newState != STATE_DISABLED &&
        newState != STATE_INACTIVE)
    {
        Console.WriteLine("[STATE] Estado inválido.");
        return;
    }

    if (newState == STATE_ACTIVE && !CanManuallySetActive(sensorId, out string? activeBlockReason))
    {
        Console.WriteLine($"[STATE] Não é possível colocar {sensorId} em ativo porque {activeBlockReason}.");
        return;
    }

    UpdateSensorEntry(sensorId, newState, DateTime.UtcNow);
    sensorStatusReasons[sensorId] = $"Estado alterado manualmente para {newState}.";

    if (newState == STATE_MAINTENANCE || newState == STATE_DISABLED)
    {
        manualStateOverrides[sensorId] = newState;
    }
    else
    {
        manualStateOverrides.TryRemove(sensorId, out _);
    }

    if (newState == STATE_INACTIVE || newState == STATE_DISABLED)
    {
        lastSeenUtc.TryRemove(sensorId, out _);
        heartbeatCounters.TryRemove(sensorId, out _);
    }

    Console.WriteLine($"[STATE] Sensor {sensorId} atualizado para {newState}.");
}

void CreateSensorProfile(string sensorId, string zona, List<string> tipos)
{
    string profilesDirectory = Path.GetFullPath(Path.Combine(Environment.CurrentDirectory, SENSOR_PROFILES_RELATIVE_PATH));
    Directory.CreateDirectory(profilesDirectory);

    string profilePath = Path.Combine(profilesDirectory, $"{sensorId}.txt");
    File.WriteAllLines(profilePath, new[]
    {
        $"sensor_id={sensorId}",
        $"zona={zona}",
        $"tipos_dados={string.Join(',', tipos)}"
    });

    Console.WriteLine($"[CREATE] Perfil criado em {profilePath}");
}

class SensorInfo
{
    public string SensorId { get; set; } = "";
    public string Estado { get; set; } = "";
    public string Zona { get; set; } = "";
    public List<string> TiposDados { get; set; } = new();
    public string LastSync { get; set; } = "-";
}

class SensorSession
{
    private readonly StreamWriter writer;
    private TaskCompletionSource<string>? pendingVideoAck;

    public SensorSession(string sensorId, StreamWriter writer)
    {
        SensorId = sensorId;
        this.writer = writer;
    }

    public string SensorId { get; }
    public SemaphoreSlim WriteLock { get; } = new(1, 1);

    public async Task SendAsync(string message)
    {
        await WriteLock.WaitAsync();
        try
        {
            await writer.WriteLineAsync(message);
        }
        finally
        {
            WriteLock.Release();
        }
    }

    public TaskCompletionSource<string> CreatePendingVideoAck()
    {
        TaskCompletionSource<string> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        Interlocked.Exchange(ref pendingVideoAck, tcs);
        return tcs;
    }

    public void TryCompleteVideoAck(string payload)
    {
        TaskCompletionSource<string>? current = Interlocked.Exchange(ref pendingVideoAck, null);
        current?.TrySetResult(payload);
    }

    public void ClearPendingVideoAck(TaskCompletionSource<string> expected)
    {
        TaskCompletionSource<string>? current = Interlocked.CompareExchange(ref pendingVideoAck, null, expected);
        if (current == expected)
        {
            expected.TrySetCanceled();
        }
    }
}
