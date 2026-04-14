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
const int MAX_BATCH_SIZE = 20;
const int BATCH_TIMEOUT_MS = 300000;
const int SENSOR_TIMEOUT_MS = 30000;
const int MONITOR_INTERVAL_MS = 5000;
const string STATE_ACTIVE = "ativo";
const string STATE_MAINTENANCE = "manutencao";
const string STATE_DISABLED = "desativado";
const string STATE_INACTIVE = "inativo";

List<string> dataBuffer = new();
Lock dataBufferLock = new();
Lock csvLock = new();
Lock historyLock = new();
Lock pendingFilesLock = new();
SemaphoreSlim batchSendLock = new(1, 1);
ConcurrentDictionary<string, DateTime> lastSeenUtc = new(StringComparer.OrdinalIgnoreCase);
ConcurrentDictionary<string, int> heartbeatCounters = new(StringComparer.OrdinalIgnoreCase);
ConcurrentDictionary<string, string> sensorStatusReasons = new(StringComparer.OrdinalIgnoreCase);

EnsureCsvExists();
LoadPendingBuffer();

TcpListener listener = new(IPAddress.Any, GATEWAY_PORT);
listener.Start();

Console.WriteLine($"[GATEWAY {GATEWAY_ID}] Ativo na porta {GATEWAY_PORT}...");
Console.WriteLine("Comandos disponíveis: 'send' para forçar envio do lote e 'status' para listar sensores.");

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

                        if (info == null)
                        {
                            await writer.WriteLineAsync("CONN_ACK|RECUSADO|ID_DESCONHECIDO");
                            break;
                        }

                        if (info.Estado == STATE_DISABLED)
                        {
                            await writer.WriteLineAsync("CONN_ACK|RECUSADO|SENSOR_DESATIVADO");
                            break;
                        }

                        if (!info.Zona.Equals(zona, StringComparison.OrdinalIgnoreCase))
                        {
                            await writer.WriteLineAsync("CONN_ACK|RECUSADO|ZONA_INVALIDA");
                            break;
                        }

                        List<string> tiposSolicitados = tiposSolicitadosRaw
                            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                            .ToList();

                        if (tiposSolicitados.Count == 0)
                        {
                            await writer.WriteLineAsync("CONN_ACK|RECUSADO|SEM_TIPOS");
                            break;
                        }

                        bool todosOsTiposSaoValidos = tiposSolicitados.All(ts =>
                            info.TiposDados.Any(csvT => csvT.Equals(ts, StringComparison.OrdinalIgnoreCase)));

                        if (!todosOsTiposSaoValidos)
                        {
                            await writer.WriteLineAsync("CONN_ACK|RECUSADO|TIPO_NAO_AUTORIZADO");
                            Console.WriteLine($"[RECUSADO] Sensor {id} tentou tipos não autorizados: {tiposSolicitadosRaw}");
                            break;
                        }

                        currentSensorId = id;
                        currentTipos = new HashSet<string>(tiposSolicitados, StringComparer.OrdinalIgnoreCase);
                        isConnected = true;
                        RegisterSensorSeen(id);
                        sensorStatusReasons[id] = info.Estado == STATE_MAINTENANCE ? "Manutenção em curso." : "Ligação aceite.";
                        UpdateSensorEntry(id, info.Estado, DateTime.UtcNow);

                        string response = info.Estado == STATE_MAINTENANCE
                            ? "CONN_ACK|MANUTENCAO"
                            : "CONN_ACK|ACEITE";

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
                        UpdateSensorEntry(currentSensorId, STATE_ACTIVE, DateTime.UtcNow);

                        Console.WriteLine($"[RECEBIDO] Sensor {currentSensorId} enviou {tipoDado}: {valor}");
                        await writer.WriteLineAsync("DATA_ACK|SUCESSO");
                        break;

                    case "HEARTBEAT":
                        if (!isConnected || currentSensorId == null)
                        {
                            await writer.WriteLineAsync("ACK_HEARTBEAT|ERRO|NAO_LIGADO");
                            break;
                        }

                        RegisterSensorSeen(currentSensorId);
                        UpdateSensorEntry(currentSensorId, STATE_ACTIVE, DateTime.UtcNow);
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

                        RegisterSensorSeen(currentSensorId);

                        if (notificationType == "LOW_BATTERY")
                        {
                            Console.WriteLine($"[ALERTA] Bateria baixa no sensor {currentSensorId}: {notificationPayload}");
                            sensorStatusReasons[currentSensorId] = $"Bateria baixa ({notificationPayload}).";
                            UpdateSensorEntry(currentSensorId, STATE_MAINTENANCE, DateTime.UtcNow);
                            await writer.WriteLineAsync("ACK_NOTIFY|SUCESSO|MANUTENCAO");
                        }
                        else if (notificationType == "CHARGING" &&
                                 notificationPayload.Equals("INICIADO", StringComparison.OrdinalIgnoreCase))
                        {
                            Console.WriteLine($"[CARREGAMENTO] Sensor {currentSensorId} entrou em carregamento.");
                            sensorStatusReasons[currentSensorId] = "Sensor em carregamento.";
                            UpdateSensorEntry(currentSensorId, STATE_MAINTENANCE, DateTime.UtcNow);
                            await writer.WriteLineAsync("ACK_NOTIFY|SUCESSO|A_CARREGAR");
                        }
                        else if (notificationType == "CHARGING" &&
                                 notificationPayload.Equals("COMPLETO", StringComparison.OrdinalIgnoreCase))
                        {
                            Console.WriteLine($"[CARREGAMENTO] Sensor {currentSensorId} terminou o carregamento.");
                            sensorStatusReasons[currentSensorId] = "Carregamento concluído.";
                            UpdateSensorEntry(currentSensorId, STATE_ACTIVE, DateTime.UtcNow);
                            await writer.WriteLineAsync("ACK_NOTIFY|SUCESSO|ATIVO");
                        }
                        else
                        {
                            await writer.WriteLineAsync("ACK_NOTIFY|SUCESSO|IGNORADO");
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

            batch = new List<string>(dataBuffer);
        }

        string msg = $"DATA_BATCH|{GATEWAY_ID}|{batch.Count}|{string.Join("#", batch)}";

        try
        {
            using TcpClient server = new();
            await server.ConnectAsync(SERVER_IP, SERVER_PORT);

            using NetworkStream stream = server.GetStream();
            using StreamWriter sw = new(stream, Encoding.UTF8) { AutoFlush = true };
            using StreamReader sr = new(stream, Encoding.UTF8);

            await sw.WriteLineAsync(msg);
            string? ack = await sr.ReadLineAsync();

            if (ack != "LOTE_ACK|SUCESSO")
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
    }
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
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
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

class SensorInfo
{
    public string SensorId { get; set; } = "";
    public string Estado { get; set; } = "";
    public string Zona { get; set; } = "";
    public List<string> TiposDados { get; set; } = new();
    public string LastSync { get; set; } = "-";
}
