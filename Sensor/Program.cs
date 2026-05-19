using DataGenerator;
using RabbitMQ.Client;
using System.Collections.Concurrent;
using System.Globalization;
using System.Net.Sockets;
using System.Text;
using System.Threading;

const string DEFAULT_GATEWAY_IP = "127.0.0.1";
const int DEFAULT_GATEWAY_PORT = 5000;
const string RABBITMQ_HOST = "localhost";
const string RABBITMQ_EXCHANGE = "onehealth.medicoes";
const int HEARTBEAT_INTERVAL_MS = 10000;
const int BATTERY_DRAIN_INTERVAL_MS = 45000;
const int BATTERY_CHARGE_INTERVAL_MS = 5000;
const int DEFAULT_VIDEO_FRAME_COUNT = 10;
const int LOW_BATTERY_THRESHOLD = 20;
const int VIDEO_STREAM_BATTERY_COST = 5;
const int VIDEO_STREAM_FRAME_DELAY_MS = 500;
const int HEARTBEAT_BATTERY_COST = 1;
const int DATA_BATTERY_COST = 3;
const int IDLE_BATTERY_COST = 1;
const int CHARGE_STEP = 10;
const string PROFILES_FOLDER = "profiles";
const string STATE_FOLDER = "state";
const string REASON_OK = "Operacional";
const string REASON_LOW_BATTERY = "Bateria fraca";
const string REASON_CHARGING = "A carregar";

SensorConfig config = ReadStartupConfig();
List<GatewayEndpoint> gatewayEndpoints = ReadGatewayEndpoints();
string sensorId = config.SensorId;
string zona = config.Zona;
string tiposDados = config.TiposDados;
List<string> configuredDataTypes = tiposDados
    .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
    .Select(type => type.ToUpperInvariant())
    .ToList();

List<GatewaySession> gatewaySessions = new();

bool ligado = false;
bool modoManutencao = false;
bool heartbeatAtivo = false;
bool aCarregar = false;
bool lowBatteryAlertSent = false;
bool autoSendEnabled = false;
bool resumeAutoSendAfterCharging = false;
bool restoreAutoSendOnStartup = false;
int heartbeatCount = 0;
int batteryLevel = 100;
int autoSendIntervalSeconds = 60;
int autoSendCycleCount = 0;
Task? chargingTask = null;
Task? heartbeatTask = null;
DateTime nextAutoSendUtc = DateTime.MaxValue;
string maintenanceReason = REASON_OK;

Lock batteryLock = new();
Lock autoSendLock = new();
Lock heartbeatTaskLock = new();
Lock videoStateLock = new();
SemaphoreSlim autoSendRoundLock = new(1, 1);
bool videoStreamActive = false;

LoadSensorState();

try
{
    string connectMsg = $"CONNECT|{sensorId}|{tiposDados}|{zona}";
    List<string> acceptedResponses = new();

    foreach (GatewayEndpoint endpoint in gatewayEndpoints)
    {
        GatewaySession session = new(endpoint);

        try
        {
            string? response = await session.ConnectAsync(connectMsg, HandleVideoRequestAsync);
            Console.WriteLine($"[SENSOR] Resposta de {endpoint}: {response}");

            if (response == "CONN_ACK|ACEITE" || response == "CONN_ACK|MANUTENCAO")
            {
                gatewaySessions.Add(session);
                acceptedResponses.Add(response);
            }
            else
            {
                await session.CloseAsync();
            }
        }
        catch (Exception ex)
        {
            await session.CloseAsync();
            Console.WriteLine($"[SENSOR] Erro ao ligar ao gateway {endpoint}: {ex.Message}");
        }
    }

    if (gatewaySessions.Count == 0)
    {
        Console.WriteLine("[SENSOR] Nenhum gateway aceitou a ligação. O programa vai terminar.");
        await FecharLigacaoLocal();
        return;
    }

    bool hasNormalConnection = acceptedResponses.Any(response =>
        response.Equals("CONN_ACK|ACEITE", StringComparison.OrdinalIgnoreCase));

    if (hasNormalConnection)
    {
        ligado = true;
        modoManutencao = batteryLevel <= LOW_BATTERY_THRESHOLD;
        heartbeatAtivo = !modoManutencao;
        Console.WriteLine($"[SENSOR] Ligação aceite em {gatewaySessions.Count} gateway(s).");
        if (modoManutencao)
        {
            maintenanceReason = REASON_LOW_BATTERY;
            Console.WriteLine($"[SENSOR] Arranque em manutenção por bateria a {batteryLevel}%.");
            _ = Task.Run(() => TrySendNotificationAsync("LOW_BATTERY", $"{batteryLevel}%"));
        }
        else
        {
            maintenanceReason = REASON_OK;
            EnsureHeartbeatLoopRunning();
            if (restoreAutoSendOnStartup)
            {
                SetAutoSendEnabled(true);
                Console.WriteLine($"[SENSOR] Envio automático restaurado no arranque com intervalo de {GetAutoSendIntervalSeconds()} segundos.");
                ScheduleNextAutoSend(immediate: false);
                _ = Task.Run(() => RunAutoSendRoundAsync("arranque"));
            }
        }
    }
    else
    {
        ligado = true;
        modoManutencao = true;
        heartbeatAtivo = false;
        if (batteryLevel <= LOW_BATTERY_THRESHOLD)
        {
            maintenanceReason = REASON_LOW_BATTERY;
        }
        Console.WriteLine($"[SENSOR] Ligação aceite em modo manutenção por {gatewaySessions.Count} gateway(s).");
    }

    _ = Task.Run(BatteryDrainLoop);
    _ = Task.Run(AutoSendLoop);
}
catch (Exception ex)
{
    Console.WriteLine($"[SENSOR] Erro ao ligar ao gateway: {ex.Message}");
    return;
}

while (true)
{
    Console.WriteLine();
    Console.WriteLine("===== SENSOR =====");
    Console.WriteLine($"Bateria: {GetBatteryLevel()}%");
    Console.WriteLine($"Estado: {GetDisplayedState()}");
    Console.WriteLine($"Carregamento: {(aCarregar ? "sim" : "não")}");
    Console.WriteLine($"Motivo: {maintenanceReason}");
    Console.WriteLine($"Envio automático: {(IsAutoSendEnabled() ? $"ativo ({GetAutoSendIntervalSeconds()}s)" : "desativado")}");
    Console.WriteLine($"Tipos configurados: {string.Join(", ", configuredDataTypes)}");
    Console.WriteLine("1 - Enviar medição manual");
    Console.WriteLine("2 - Colocar a carregar");
    Console.WriteLine("3 - Ver bateria");
    Console.WriteLine("4 - Ativar/desativar envio automático");
    Console.WriteLine("5 - Alterar intervalo do envio automático");
    Console.WriteLine("6 - Enviar vídeo simulado");
    Console.WriteLine("7 - Desligar");
    Console.Write("Opção: ");

    string? opcao = Console.ReadLine();

    try
    {
        switch (opcao)
        {
            case "1":
                await EnviarMedicaoManual();
                break;

            case "2":
                await IniciarCarregamento();
                break;

            case "3":
                Console.WriteLine($"[SENSOR] Bateria atual: {GetBatteryLevel()}%");
                break;

            case "4":
                ToggleAutoSend();
                break;

            case "5":
                ConfigureAutoSendInterval();
                break;

            case "6":
                await IniciarEnvioVideoAsync();
                break;

            case "7":
                await DesligarSensor();
                return;

            default:
                Console.WriteLine("Opção inválida.");
                break;
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[SENSOR] Erro: {ex.Message}");
        await FecharLigacaoLocal();
        return;
    }
}

async Task EnviarMedicaoManual()
{
    Console.Write("Tipo de dado: ");
    string tipo = (Console.ReadLine()?.Trim() ?? "TEMP").ToUpperInvariant();

    Console.Write($"Valor (ex: 25, {tipo}=25, {tipo}-25, {tipo}.25, {tipo}/25): ");
    string valorInput = Console.ReadLine()?.Trim() ?? "0";

    if (string.IsNullOrWhiteSpace(valorInput))
    {
        Console.WriteLine("[SENSOR] Valor inválido: introduz um valor, por exemplo 25 ou TEMP=25.");
        return;
    }

    await EnviarMedicaoAsync(tipo, valorInput, origemAutomatica: false);
}

async Task EnviarMedicaoAsync(string tipo, string valor, bool origemAutomatica)
{
    if (!CanUseConnection())
    {
        Console.WriteLine("O sensor não está ligado ao gateway.");
        return;
    }

    if (!CanSendMeasurement(origemAutomatica))
    {
        return;
    }

    if (GetBatteryLevel() <= 0)
    {
        Console.WriteLine("[SENSOR] Bateria esgotada. Coloque o sensor a carregar.");
        EnterMaintenance(REASON_LOW_BATTERY, preserveAutoSendPreference: true);
        return;
    }

    if (!configuredDataTypes.Contains(tipo, StringComparer.OrdinalIgnoreCase))
    {
        Console.WriteLine($"[SENSOR] O tipo {tipo} não está configurado neste sensor.");
        return;
    }

    string timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");

    bool shouldCheckLowBattery = false;

    string dataMsg = $"DATA|{sensorId}|{timestamp}|{tipo}|{valor}";
    string topic = BuildDataTopic(tipo);
    string? response = await PublishMeasurementAsync(topic, dataMsg);
    Console.WriteLine($"[SENSOR] Resposta Pub/Sub: {response}");

    if (response != null && response.StartsWith("PUB_ACK|SUCESSO", StringComparison.OrdinalIgnoreCase))
    {
        ConsumeBattery(DATA_BATTERY_COST);
        shouldCheckLowBattery = true;
        SaveSensorState();
    }

    if (shouldCheckLowBattery)
    {
        await CheckLowBatteryAsync();
    }
}

async Task<string?> PublishMeasurementAsync(string topic, string payload)
{
    try
    {
        await Task.Run(() =>
        {
            ConnectionFactory factory = new() { HostName = RABBITMQ_HOST };
            using IConnection connection = factory.CreateConnection();
            using IModel channel = connection.CreateModel();

            channel.ExchangeDeclare(
                exchange: RABBITMQ_EXCHANGE,
                type: ExchangeType.Topic,
                durable: true,
                autoDelete: false);

            IBasicProperties properties = channel.CreateBasicProperties();
            properties.Persistent = true;
            properties.ContentType = "text/plain";
            properties.Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds());

            byte[] body = Encoding.UTF8.GetBytes(payload);
            channel.ConfirmSelect();
            channel.BasicPublish(
                exchange: RABBITMQ_EXCHANGE,
                routingKey: topic,
                basicProperties: properties,
                body: body);
            channel.WaitForConfirmsOrDie(TimeSpan.FromSeconds(5));
        }).WaitAsync(TimeSpan.FromSeconds(8));

        return "PUB_ACK|SUCESSO|RABBITMQ";
    }
    catch (Exception ex)
    {
        return $"PUB_ACK|ERRO|RABBITMQ_INDISPONIVEL:{ex.Message}";
    }
}

string BuildDataTopic(string tipo)
{
    return $"DATA.{sensorId.ToUpperInvariant()}.{zona.ToUpperInvariant()}.{tipo.ToUpperInvariant()}";
}

async Task IniciarEnvioVideoAsync()
{
    if (!CanUseConnection())
    {
        Console.WriteLine("[VIDEO] O sensor não está ligado ao gateway.");
        return;
    }

    if (aCarregar || modoManutencao)
    {
        Console.WriteLine("[VIDEO] O sensor não pode iniciar vídeo no estado atual.");
        return;
    }

    if (GetBatteryLevel() <= LOW_BATTERY_THRESHOLD)
    {
        Console.WriteLine("[VIDEO] Bateria insuficiente para iniciar vídeo.");
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

    GatewaySession? selectedGateway = SelectGatewaySession("enviar vídeo");
    if (selectedGateway == null)
    {
        return;
    }

    string request = $"VIDEO_REQ|{sensorId}|{frameCount}";
    string? response = await selectedGateway.SendRequestAsync(request);

    if (response == null || !response.StartsWith("VIDEO_ACK|ACEITE|", StringComparison.OrdinalIgnoreCase))
    {
        Console.WriteLine($"[VIDEO] Pedido recusado pelo gateway {selectedGateway.Endpoint}: {response ?? "<sem resposta>"}");
        return;
    }

    string[] parts = response.Split('|');
    if (parts.Length < 3 || !int.TryParse(parts[2], out int streamPort))
    {
        Console.WriteLine($"[VIDEO] Resposta inválida do gateway: {response}");
        return;
    }

    Console.WriteLine($"[VIDEO] Pedido aceite por {selectedGateway.Endpoint}. A iniciar stream para a porta {streamPort}...");
    _ = Task.Run(() => StreamVideoAsync(selectedGateway.Endpoint.Host, streamPort, frameCount));
}

void ToggleAutoSend()
{
    if (aCarregar)
    {
        Console.WriteLine("[SENSOR] Não é possível ativar envio automático durante o carregamento.");
        return;
    }

    bool newState = !IsAutoSendEnabled();
    SetAutoSendEnabled(newState);
    if (!newState)
    {
        resumeAutoSendAfterCharging = false;
        ClearNextAutoSend();
    }

    Console.WriteLine(newState
        ? $"[SENSOR] Envio automático ativado com intervalo de {GetAutoSendIntervalSeconds()} segundos."
        : "[SENSOR] Envio automático desativado.");

    if (newState)
    {
        ScheduleNextAutoSend(immediate: false);
        _ = Task.Run(() => RunAutoSendRoundAsync("manual"));
    }

    SaveSensorState();
}

void ConfigureAutoSendInterval()
{
    Console.Write("Novo intervalo em segundos: ");
    string? input = Console.ReadLine()?.Trim();

    if (!int.TryParse(input, out int seconds) || seconds < 5)
    {
        Console.WriteLine("[SENSOR] Intervalo inválido. Usa um valor inteiro igual ou superior a 5.");
        return;
    }

    SetAutoSendIntervalSeconds(seconds);
    Console.WriteLine($"[SENSOR] Intervalo de envio automático definido para {seconds} segundos.");

    if (IsAutoSendEnabled())
    {
        ScheduleNextAutoSend(immediate: false);
    }

    SaveSensorState();
}

async Task AutoSendLoop()
{
    while (true)
    {
        await Task.Delay(500);

        if (!ligado || !IsAutoSendEnabled() || aCarregar || modoManutencao || !ShouldRunAutoSendNow())
        {
            continue;
        }

        await RunAutoSendRoundAsync("intervalo");
    }
}

Task IniciarCarregamento()
{
    if (!CanUseConnection())
    {
        Console.WriteLine("O sensor não está ligado ao gateway.");
        return Task.CompletedTask;
    }

    if (aCarregar)
    {
        Console.WriteLine("[SENSOR] O sensor já está a carregar.");
        return Task.CompletedTask;
    }

    aCarregar = true;
    modoManutencao = true;
    heartbeatAtivo = false;
    maintenanceReason = REASON_CHARGING;
    RememberAutoSendPreference();
    SetAutoSendEnabled(false);
    ClearNextAutoSend();
    SaveSensorState();

    Console.WriteLine("[SENSOR] Carregamento iniciado.");
    _ = Task.Run(() => TrySendNotificationAsync("CHARGING", "INICIADO"));

    if (chargingTask == null || chargingTask.IsCompleted)
    {
        chargingTask = Task.Run(ChargingLoop);
    }

    return Task.CompletedTask;
}

async Task ChargingLoop()
{
    while (aCarregar)
    {
        await Task.Delay(BATTERY_CHARGE_INTERVAL_MS);

        bool completed = false;
        int currentLevel;

        lock (batteryLock)
        {
            batteryLevel = Math.Min(100, batteryLevel + CHARGE_STEP);
            currentLevel = batteryLevel;
            completed = batteryLevel >= 100;
        }
        SaveSensorState();

        Console.WriteLine($"[SENSOR] A carregar... bateria a {currentLevel}%.");

        if (completed)
        {
            RestoreActiveModeAfterCharging();
            bool restoreAutoSend = resumeAutoSendAfterCharging;
            resumeAutoSendAfterCharging = false;

            Console.WriteLine("[SENSOR] Carregamento concluído. Sensor novamente ativo.");
            _ = Task.Run(() => TrySendNotificationAsync("CHARGING", "COMPLETO"));
            EnsureHeartbeatLoopRunning();
            if (restoreAutoSend)
            {
                SetAutoSendEnabled(true);
                Console.WriteLine($"[SENSOR] Envio automático restaurado com intervalo de {GetAutoSendIntervalSeconds()} segundos.");
                ScheduleNextAutoSend(immediate: false);
                _ = Task.Run(() => RunAutoSendRoundAsync("restauro"));
            }
        }
    }
}

async Task HeartbeatLoop()
{
    while (heartbeatAtivo)
    {
        try
        {
            await Task.Delay(HEARTBEAT_INTERVAL_MS);

            if (!heartbeatAtivo || !ligado || !gatewaySessions.Any(session => session.IsConnected) || aCarregar)
            {
                continue;
            }

            if (GetBatteryLevel() <= 0)
            {
                EnterMaintenance(REASON_LOW_BATTERY, preserveAutoSendPreference: true);
                Console.WriteLine("[SENSOR] Heartbeat parado por bateria esgotada.");
                continue;
            }

            bool shouldCheckLowBattery = false;

            string timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");
            string hbMsg = $"HEARTBEAT|{sensorId}|{timestamp}";
            int successfulHeartbeats = 0;

            foreach (GatewaySession session in gatewaySessions.Where(session => session.IsConnected).ToList())
            {
                string? response = await session.SendRequestAsync(hbMsg, timeoutSeconds: 5);
                if (response == null || !response.StartsWith("ACK_HEARTBEAT|SUCESSO", StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine($"[SENSOR] Erro no heartbeat para {session.Endpoint}: {response}");
                }
                else
                {
                    successfulHeartbeats++;
                }
            }

            if (successfulHeartbeats > 0)
            {
                heartbeatCount++;
                ConsumeBattery(HEARTBEAT_BATTERY_COST);
                shouldCheckLowBattery = true;
                SaveSensorState();

                if (heartbeatCount % 10 == 0)
                {
                    Console.WriteLine($"[SENSOR] {heartbeatCount} rondas de heartbeat enviadas para {successfulHeartbeats} gateway(s).");
                }
            }

            if (shouldCheckLowBattery)
            {
                await CheckLowBatteryAsync();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[SENSOR] Erro no heartbeat: {ex.Message}");

            if (!ligado || !heartbeatAtivo)
            {
                break;
            }

            await Task.Delay(1000);
        }
    }
}

async Task HandleVideoRequestAsync(GatewaySession session, string requestLine)
{
    string[] parts = requestLine.Split('|');
    if (parts.Length < 4)
    {
        await SendVideoAckAsync(session, "RECUSADO|FORMATO_INVALIDO");
        return;
    }

    string requestedSensorId = parts[1].Trim();
    bool portValid = int.TryParse(parts[2], out int streamPort);
    bool framesValid = int.TryParse(parts[3], out int frameCount);

    if (!requestedSensorId.Equals(sensorId, StringComparison.OrdinalIgnoreCase) ||
        !portValid ||
        !framesValid ||
        streamPort <= 0 ||
        frameCount <= 0)
    {
        await SendVideoAckAsync(session, "RECUSADO|PEDIDO_INVALIDO");
        return;
    }

    lock (videoStateLock)
    {
        if (videoStreamActive)
        {
            _ = SendVideoAckAsync(session, "RECUSADO|STREAM_OCUPADA");
            return;
        }

        videoStreamActive = true;
    }

    if (!ligado || aCarregar || modoManutencao || GetBatteryLevel() <= LOW_BATTERY_THRESHOLD)
    {
        lock (videoStateLock)
        {
            videoStreamActive = false;
        }

        await SendVideoAckAsync(session, "RECUSADO|SENSOR_INDISPONIVEL");
        return;
    }

    await SendVideoAckAsync(session, $"ACEITE|{streamPort}");
    _ = Task.Run(() => StreamVideoAsync(session.Endpoint.Host, streamPort, frameCount));
}

async Task SendVideoAckAsync(GatewaySession session, string payload)
{
    if (!CanUseConnection())
    {
        return;
    }

    await session.SendOneWayAsync($"VIDEO_ACK|{sensorId}|{payload}");
}

async Task StreamVideoAsync(string gatewayHost, int streamPort, int frameCount)
{
    try
    {
        using TcpClient streamClient = new();
        await streamClient.ConnectAsync(gatewayHost, streamPort);

        using NetworkStream stream = streamClient.GetStream();
        using StreamWriter streamWriter = new(stream, Encoding.UTF8) { AutoFlush = true };

        string startedAt = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");
        await streamWriter.WriteLineAsync($"VIDEO_START|{sensorId}|{startedAt}|{frameCount}");

        for (int i = 1; i <= frameCount; i++)
        {
            if (!ligado || aCarregar || modoManutencao)
            {
                break;
            }

            string timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");
            string frameId = $"FRAME_{i:D3}";
            string payload = BuildSimulatedFramePayload(i);
            await streamWriter.WriteLineAsync($"VIDEO_FRAME|{sensorId}|{timestamp}|{frameId}|{payload}");
            await Task.Delay(VIDEO_STREAM_FRAME_DELAY_MS);
        }

        string endedAt = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");
        await streamWriter.WriteLineAsync($"VIDEO_END|{sensorId}|{endedAt}");

        ConsumeBattery(VIDEO_STREAM_BATTERY_COST);
        SaveSensorState();
        await CheckLowBatteryAsync();
        Console.WriteLine($"[VIDEO] Stream simulada enviada com {frameCount} frames.");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[VIDEO] Falha ao enviar stream: {ex.Message}");
    }
    finally
    {
        lock (videoStateLock)
        {
            videoStreamActive = false;
        }
    }
}

string BuildSimulatedFramePayload(int frameNumber)
{
    string temp = configuredDataTypes.Contains("TEMP", StringComparer.OrdinalIgnoreCase)
        ? SensorDataGenerator.Generate("TEMP")
        : "NA";
    string hum = configuredDataTypes.Contains("HUM", StringComparer.OrdinalIgnoreCase)
        ? SensorDataGenerator.Generate("HUM")
        : "NA";
    string ruido = configuredDataTypes.Contains("RUIDO", StringComparer.OrdinalIgnoreCase)
        ? SensorDataGenerator.Generate("RUIDO")
        : "NA";

    return $"scene=SIMULADA;frame={frameNumber:D3};temp={temp};hum={hum};ruido={ruido}";
}

void EnsureHeartbeatLoopRunning()
{
    lock (heartbeatTaskLock)
    {
        if (!heartbeatAtivo)
        {
            heartbeatAtivo = true;
        }

        if (heartbeatTask == null || heartbeatTask.IsCompleted)
        {
            heartbeatTask = Task.Run(HeartbeatLoop);
        }
    }
}

async Task BatteryDrainLoop()
{
    while (ligado)
    {
        await Task.Delay(BATTERY_DRAIN_INTERVAL_MS);

        if (!ligado || aCarregar)
        {
            continue;
        }

        ConsumeBattery(IDLE_BATTERY_COST);
        SaveSensorState();
        await CheckLowBatteryAsync();
    }
}

async Task CheckLowBatteryAsync()
{
    int currentBattery = GetBatteryLevel();

    if (currentBattery > LOW_BATTERY_THRESHOLD)
    {
        lowBatteryAlertSent = false;
        if (!aCarregar && !modoManutencao)
        {
            maintenanceReason = REASON_OK;
        }
        SaveSensorState();
        return;
    }

    if (lowBatteryAlertSent || aCarregar)
    {
        if (currentBattery <= 0)
        {
            EnterMaintenance(REASON_LOW_BATTERY, preserveAutoSendPreference: true);
            Console.WriteLine("[SENSOR] Bateria esgotada. O sensor precisa de carregamento.");
        }
        return;
    }

    EnterMaintenance(REASON_LOW_BATTERY, preserveAutoSendPreference: true);
    lowBatteryAlertSent = true;

    Console.WriteLine($"[SENSOR] Bateria baixa ({currentBattery}%). A notificar o gateway.");
    await SendNotificationAsync("LOW_BATTERY", $"{currentBattery}%");

    if (currentBattery <= 0)
    {
        Console.WriteLine("[SENSOR] Bateria esgotada. O sensor precisa de carregamento.");
    }
}

async Task RunAutoSendRoundAsync(string source)
{
    if (!await autoSendRoundLock.WaitAsync(0))
    {
        return;
    }

    try
    {
        if (!ligado || !IsAutoSendEnabled() || aCarregar || modoManutencao)
        {
            return;
        }

        ClearNextAutoSend();
        autoSendCycleCount++;
        Console.WriteLine($"[AUTO] Início da ronda {autoSendCycleCount} ({source}).");

        foreach (string dataType in configuredDataTypes)
        {
            if (!ligado || !IsAutoSendEnabled() || aCarregar || modoManutencao)
            {
                break;
            }

            string generatedValue = SensorDataGenerator.Generate(dataType);
            Console.WriteLine($"[AUTO] {dataType}={generatedValue}");
            await EnviarMedicaoAsync(dataType, generatedValue, origemAutomatica: true);
        }

        Console.WriteLine($"[AUTO] Fim da ronda {autoSendCycleCount}.");

        if (ligado && IsAutoSendEnabled() && !aCarregar && !modoManutencao)
        {
            ScheduleNextAutoSend(immediate: false);
        }
    }
    finally
    {
        autoSendRoundLock.Release();
    }
}

void RememberAutoSendPreference()
{
    if (IsAutoSendEnabled() || resumeAutoSendAfterCharging)
    {
        resumeAutoSendAfterCharging = true;
    }
}

string GetDisplayedState()
{
    if (!ligado)
    {
        return "inativo";
    }

    if (modoManutencao)
    {
        return "manutencao";
    }

    return "ativo";
}

bool CanUseConnection()
{
    return ligado && gatewaySessions.Any(session => session.IsConnected);
}

bool CanSendMeasurement(bool origemAutomatica)
{
    if (aCarregar)
    {
        Console.WriteLine("[SENSOR] Não é possível enviar medições enquanto a bateria está a carregar.");
        return false;
    }

    if (modoManutencao && !origemAutomatica)
    {
        Console.WriteLine("[SENSOR] O sensor está em manutenção.");
        return false;
    }

    return true;
}

void EnterMaintenance(string reason, bool preserveAutoSendPreference)
{
    modoManutencao = true;
    heartbeatAtivo = false;
    maintenanceReason = reason;

    if (preserveAutoSendPreference)
    {
        RememberAutoSendPreference();
    }

    SetAutoSendEnabled(false);
    ClearNextAutoSend();
    SaveSensorState();
}

void RestoreActiveModeAfterCharging()
{
    aCarregar = false;
    modoManutencao = false;
    lowBatteryAlertSent = false;
    heartbeatAtivo = true;
    maintenanceReason = REASON_OK;
    SaveSensorState();
}

bool ShouldRunAutoSendNow()
{
    lock (autoSendLock)
    {
        return autoSendEnabled && nextAutoSendUtc != DateTime.MaxValue && DateTime.UtcNow >= nextAutoSendUtc;
    }
}

void ScheduleNextAutoSend(bool immediate)
{
    lock (autoSendLock)
    {
        nextAutoSendUtc = immediate
            ? DateTime.UtcNow
            : DateTime.UtcNow.AddSeconds(autoSendIntervalSeconds);
    }
}

void ClearNextAutoSend()
{
    lock (autoSendLock)
    {
        nextAutoSendUtc = DateTime.MaxValue;
    }
}

void LoadSensorState()
{
    string statePath = GetSensorStatePath();
    Directory.CreateDirectory(Path.GetDirectoryName(statePath)!);

    if (!File.Exists(statePath))
    {
        batteryLevel = 100;
        maintenanceReason = REASON_OK;
        return;
    }

    Dictionary<string, string> values = File.ReadAllLines(statePath)
        .Where(line => !string.IsNullOrWhiteSpace(line) && !line.TrimStart().StartsWith('#'))
        .Select(line => line.Split('=', 2))
        .Where(parts => parts.Length == 2)
        .ToDictionary(parts => parts[0].Trim().ToLowerInvariant(), parts => parts[1].Trim());

    if (int.TryParse(values.GetValueOrDefault("battery_level"), out int savedBattery))
    {
        batteryLevel = Math.Clamp(savedBattery, 0, 100);
    }

    if (int.TryParse(values.GetValueOrDefault("auto_send_interval_seconds"), out int savedInterval) && savedInterval >= 5)
    {
        autoSendIntervalSeconds = savedInterval;
    }

    if (bool.TryParse(values.GetValueOrDefault("auto_send_enabled"), out bool savedAutoSendEnabled))
    {
        restoreAutoSendOnStartup = savedAutoSendEnabled;
    }

    if (bool.TryParse(values.GetValueOrDefault("resume_auto_send_after_charging"), out bool savedResumeAutoSend))
    {
        resumeAutoSendAfterCharging = savedResumeAutoSend;
    }

    maintenanceReason = values.GetValueOrDefault("maintenance_reason",
        batteryLevel <= LOW_BATTERY_THRESHOLD ? REASON_LOW_BATTERY : REASON_OK);
    modoManutencao = batteryLevel <= LOW_BATTERY_THRESHOLD;
}

void SaveSensorState()
{
    string statePath = GetSensorStatePath();
    Directory.CreateDirectory(Path.GetDirectoryName(statePath)!);
    bool shouldRestoreAutoSend = IsAutoSendEnabled() || resumeAutoSendAfterCharging;

    File.WriteAllLines(statePath, new[]
    {
        $"sensor_id={sensorId}",
        $"battery_level={GetBatteryLevel()}",
        $"auto_send_enabled={shouldRestoreAutoSend}",
        $"auto_send_interval_seconds={GetAutoSendIntervalSeconds()}",
        $"resume_auto_send_after_charging={resumeAutoSendAfterCharging}",
        $"maintenance_reason={maintenanceReason}",
        $"updated_at={DateTime.Now:yyyy-MM-ddTHH:mm:ss}"
    });
}

string GetSensorStatePath()
{
    return Path.Combine(Environment.CurrentDirectory, STATE_FOLDER, $"{sensorId}.state.txt");
}

async Task SendNotificationAsync(string notificationType, string payload)
{
    if (!ligado || gatewaySessions.Count == 0)
    {
        return;
    }

    string notifyMsg = $"NOTIFY|{sensorId}|{notificationType}|{payload}";
    foreach (GatewaySession session in gatewaySessions.Where(session => session.IsConnected).ToList())
    {
        string? response = await session.SendRequestAsync(notifyMsg);
        Console.WriteLine($"[SENSOR] Resposta de {session.Endpoint}: {response}");
    }
}

async Task TrySendNotificationAsync(string notificationType, string payload)
{
    try
    {
        if (!ligado || gatewaySessions.Count == 0)
        {
            return;
        }

        string notifyMsg = $"NOTIFY|{sensorId}|{notificationType}|{payload}";
        int sentCount = 0;

        foreach (GatewaySession session in gatewaySessions.Where(session => session.IsConnected).ToList())
        {
            string? response = await session.TrySendRequestAsync(notifyMsg, timeoutSeconds: 3);
            if (response == null)
            {
                Console.WriteLine($"[SENSOR] Não foi possível notificar {notificationType} de imediato a {session.Endpoint}.");
                continue;
            }

            sentCount++;
            Console.WriteLine($"[SENSOR] Resposta de {session.Endpoint}: {response}");
        }

        if (sentCount == 0)
        {
            Console.WriteLine($"[SENSOR] Notificação {notificationType} não foi entregue a nenhum gateway.");
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[SENSOR] Falha ao enviar notificação {notificationType}: {ex.Message}");
    }
}

void ConsumeBattery(int amount)
{
    lock (batteryLock)
    {
        batteryLevel = Math.Max(0, batteryLevel - amount);
    }
}

int GetBatteryLevel()
{
    lock (batteryLock)
    {
        return batteryLevel;
    }
}

bool IsAutoSendEnabled()
{
    lock (autoSendLock)
    {
        return autoSendEnabled;
    }
}

void SetAutoSendEnabled(bool enabled)
{
    lock (autoSendLock)
    {
        autoSendEnabled = enabled;
    }
}

int GetAutoSendIntervalSeconds()
{
    lock (autoSendLock)
    {
        return autoSendIntervalSeconds;
    }
}

void SetAutoSendIntervalSeconds(int seconds)
{
    lock (autoSendLock)
    {
        autoSendIntervalSeconds = seconds;
    }
}

async Task DesligarSensor()
{
    try
    {
        heartbeatAtivo = false;
        aCarregar = false;
        SetAutoSendEnabled(false);
        await Task.Delay(300);

        if (ligado && gatewaySessions.Count > 0)
        {
            string discMsg = $"DISCONNECT|{sensorId}";
            Console.WriteLine($"[SENSOR] A enviar: {discMsg}");

            foreach (GatewaySession session in gatewaySessions.Where(session => session.IsConnected).ToList())
            {
                string? response = await session.SendRequestAsync(discMsg, timeoutSeconds: 5);
                Console.WriteLine($"[SENSOR] Resposta de {session.Endpoint}: {response}");
            }
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[SENSOR] Erro ao desligar: {ex.Message}");
    }
    finally
    {
        await FecharLigacaoLocal();
    }
}

async Task FecharLigacaoLocal()
{
    heartbeatAtivo = false;
    ligado = false;
    aCarregar = false;
    SetAutoSendEnabled(false);

    foreach (GatewaySession session in gatewaySessions)
    {
        await session.CloseAsync();
    }

    gatewaySessions.Clear();

    await Task.CompletedTask;
}

GatewaySession? SelectGatewaySession(string action)
{
    List<GatewaySession> connectedSessions = gatewaySessions
        .Where(session => session.IsConnected)
        .ToList();

    if (connectedSessions.Count == 0)
    {
        Console.WriteLine($"[SENSOR] Não existe gateway ligado para {action}.");
        return null;
    }

    if (connectedSessions.Count == 1)
    {
        return connectedSessions[0];
    }

    Console.WriteLine($"Escolhe o Gateway para {action}:");
    for (int i = 0; i < connectedSessions.Count; i++)
    {
        Console.WriteLine($"{i + 1} - {connectedSessions[i].Endpoint}");
    }

    Console.Write("Opção: ");
    string? input = Console.ReadLine()?.Trim();
    if (!int.TryParse(input, out int selectedIndex) ||
        selectedIndex < 1 ||
        selectedIndex > connectedSessions.Count)
    {
        Console.WriteLine("[SENSOR] Gateway inválido.");
        return null;
    }

    return connectedSessions[selectedIndex - 1];
}

List<GatewayEndpoint> ReadGatewayEndpoints()
{
    Console.WriteLine("Ligação de controlo aos Gateways:");
    Console.WriteLine($"1 - Gateway principal ({DEFAULT_GATEWAY_IP}:{DEFAULT_GATEWAY_PORT})");
    Console.WriteLine("2 - Escolher um Gateway específico");
    Console.WriteLine("3 - Ligar a vários Gateways");
    Console.Write("Opção (Enter para 1): ");

    string option = Console.ReadLine()?.Trim() ?? "";
    if (option == "2")
    {
        return new List<GatewayEndpoint> { ReadSingleGatewayEndpoint() };
    }

    if (option == "3")
    {
        Console.Write("Gateways (ex: 5000,5001 ou 127.0.0.1:5000;127.0.0.1:5001): ");
        string input = Console.ReadLine()?.Trim() ?? "";
        List<GatewayEndpoint> endpoints = input
            .Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(ParseGatewayEndpoint)
            .Where(endpoint => endpoint != null)
            .Cast<GatewayEndpoint>()
            .Distinct()
            .ToList();

        if (endpoints.Count > 0)
        {
            return endpoints;
        }

        Console.WriteLine("[SENSOR] Nenhum gateway válido indicado. Vai ser usado o Gateway principal.");
    }

    return new List<GatewayEndpoint> { new(DEFAULT_GATEWAY_IP, DEFAULT_GATEWAY_PORT) };
}

GatewayEndpoint ReadSingleGatewayEndpoint()
{
    Console.Write($"IP do Gateway (Enter para {DEFAULT_GATEWAY_IP}): ");
    string host = Console.ReadLine()?.Trim() ?? "";
    if (string.IsNullOrWhiteSpace(host))
    {
        host = DEFAULT_GATEWAY_IP;
    }

    Console.Write($"Porta do Gateway (Enter para {DEFAULT_GATEWAY_PORT}): ");
    string portInput = Console.ReadLine()?.Trim() ?? "";
    if (!int.TryParse(portInput, out int port) || port <= 0 || port > 65535)
    {
        port = DEFAULT_GATEWAY_PORT;
    }

    return new GatewayEndpoint(host, port);
}

GatewayEndpoint? ParseGatewayEndpoint(string input)
{
    string value = input.Trim();
    if (string.IsNullOrWhiteSpace(value))
    {
        return null;
    }

    if (int.TryParse(value, out int onlyPort) && onlyPort > 0 && onlyPort <= 65535)
    {
        return new GatewayEndpoint(DEFAULT_GATEWAY_IP, onlyPort);
    }

    string[] parts = value.Split(':', 2, StringSplitOptions.TrimEntries);
    if (parts.Length == 2 &&
        !string.IsNullOrWhiteSpace(parts[0]) &&
        int.TryParse(parts[1], out int port) &&
        port > 0 &&
        port <= 65535)
    {
        return new GatewayEndpoint(parts[0], port);
    }

    Console.WriteLine($"[SENSOR] Gateway inválido ignorado: {input}");
    return null;
}

SensorConfig ReadStartupConfig()
{
    Console.WriteLine("Modo de inicialização do sensor:");
    Console.WriteLine("1 - Introduzir dados manualmente");
    Console.WriteLine("2 - Ler a partir de ficheiro");
    Console.Write("Opção: ");

    string? option = Console.ReadLine()?.Trim();
    return option == "2" ? ReadConfigFromFile() : ReadConfigManually();
}

SensorConfig ReadConfigManually()
{
    Console.Write("ID do sensor: ");
    string sensorId = Console.ReadLine()?.Trim() ?? "S101";

    Console.Write("Zona: ");
    string zona = Console.ReadLine()?.Trim() ?? "ZONA_PARQUE";

    Console.Write("Tipos de dados (ex: TEMP,RUIDO,HUM): ");
    string tiposDados = Console.ReadLine()?.Trim() ?? "TEMP,RUIDO,HUM";

    return new SensorConfig
    {
        SensorId = sensorId,
        Zona = zona,
        TiposDados = tiposDados
    };
}

SensorConfig ReadConfigFromFile()
{
    string profilesPath = Path.Combine(Environment.CurrentDirectory, PROFILES_FOLDER);
    Directory.CreateDirectory(profilesPath);

    string[] files = Directory.GetFiles(profilesPath, "*.txt")
        .OrderBy(path => path, StringComparer.OrdinalIgnoreCase)
        .ToArray();

    if (files.Length == 0)
    {
        Console.WriteLine("[SENSOR] Não existem perfis. Vai ser usada introdução manual.");
        return ReadConfigManually();
    }

    Console.WriteLine("Perfis disponíveis:");
    for (int i = 0; i < files.Length; i++)
    {
        Console.WriteLine($"{i + 1} - {Path.GetFileNameWithoutExtension(files[i])}");
    }

    string selectedPath = "";
    while (true)
    {
        Console.Write("Escolhe o número do perfil, escreve o ID do sensor ou o caminho de um ficheiro: ");
        string? selection = Console.ReadLine()?.Trim();

        if (int.TryParse(selection, out int selectedIndex) &&
            selectedIndex >= 1 &&
            selectedIndex <= files.Length)
        {
            selectedPath = files[selectedIndex - 1];
            break;
        }

        if (!string.IsNullOrWhiteSpace(selection))
        {
            string byIdPath = Path.Combine(profilesPath, $"{selection}.txt");
            if (File.Exists(byIdPath))
            {
                selectedPath = byIdPath;
                break;
            }

            if (File.Exists(selection))
            {
                selectedPath = selection;
                break;
            }
        }

        Console.WriteLine("[SENSOR] Perfil inválido. Tenta novamente.");
    }

    Dictionary<string, string> values = File.ReadAllLines(selectedPath)
        .Where(line => !string.IsNullOrWhiteSpace(line) && !line.TrimStart().StartsWith('#'))
        .Select(line => line.Split('=', 2))
        .Where(parts => parts.Length == 2)
        .ToDictionary(parts => parts[0].Trim().ToLowerInvariant(), parts => parts[1].Trim());

    SensorConfig config = new()
    {
        SensorId = values.GetValueOrDefault("sensor_id", "S101"),
        Zona = values.GetValueOrDefault("zona", "ZONA_PARQUE"),
        TiposDados = values.GetValueOrDefault("tipos_dados", "TEMP,HUM,RUIDO")
    };

    Console.WriteLine($"[SENSOR] Perfil carregado: {Path.GetFileName(selectedPath)}");
    Console.WriteLine($"[SENSOR] ID={config.SensorId} | Zona={config.Zona} | Tipos={config.TiposDados}");

    return config;
}

class SensorConfig
{
    public string SensorId { get; set; } = "";
    public string Zona { get; set; } = "";
    public string TiposDados { get; set; } = "";
}

record GatewayEndpoint(string Host, int Port)
{
    public override string ToString() => $"{Host}:{Port}";
}

class GatewaySession
{
    private readonly SemaphoreSlim socketLock = new(1, 1);
    private readonly SemaphoreSlim responseSignal = new(0);
    private readonly ConcurrentQueue<string> responseQueue = new();
    private TcpClient? client;
    private StreamReader? reader;
    private StreamWriter? writer;

    public GatewaySession(GatewayEndpoint endpoint)
    {
        Endpoint = endpoint;
    }

    public GatewayEndpoint Endpoint { get; }
    public bool IsConnected { get; private set; }

    public async Task<string?> ConnectAsync(
        string connectMessage,
        Func<GatewaySession, string, Task> videoRequestHandler)
    {
        client = new TcpClient();
        await client.ConnectAsync(Endpoint.Host, Endpoint.Port);

        NetworkStream stream = client.GetStream();
        reader = new StreamReader(stream, Encoding.UTF8);
        writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true };
        IsConnected = true;

        _ = Task.Run(() => ReceiveLoopAsync(videoRequestHandler));
        return await SendRequestAsync(connectMessage);
    }

    public async Task<string?> SendRequestAsync(string message, int timeoutSeconds = 15)
    {
        if (!IsConnected || writer == null)
        {
            return null;
        }

        await socketLock.WaitAsync();
        try
        {
            await writer.WriteLineAsync(message);
            return await WaitForProtocolResponseAsync(timeoutSeconds);
        }
        finally
        {
            socketLock.Release();
        }
    }

    public async Task<string?> TrySendRequestAsync(string message, int timeoutSeconds = 15)
    {
        if (!IsConnected || writer == null)
        {
            return null;
        }

        bool acquired = await socketLock.WaitAsync(0);
        if (!acquired)
        {
            return null;
        }

        try
        {
            await writer.WriteLineAsync(message);
            return await WaitForProtocolResponseAsync(timeoutSeconds);
        }
        finally
        {
            socketLock.Release();
        }
    }

    public async Task SendOneWayAsync(string message)
    {
        if (!IsConnected || writer == null)
        {
            return;
        }

        await socketLock.WaitAsync();
        try
        {
            await writer.WriteLineAsync(message);
        }
        finally
        {
            socketLock.Release();
        }
    }

    public Task CloseAsync()
    {
        IsConnected = false;

        try { reader?.Dispose(); } catch { }
        try { writer?.Dispose(); } catch { }
        try { client?.Dispose(); } catch { }

        reader = null;
        writer = null;
        client = null;

        responseQueue.Enqueue("ERROR|LIGACAO_FECHADA");
        responseSignal.Release();

        return Task.CompletedTask;
    }

    private async Task ReceiveLoopAsync(Func<GatewaySession, string, Task> videoRequestHandler)
    {
        try
        {
            while (IsConnected && reader != null)
            {
                string? line = await reader.ReadLineAsync();
                if (line == null)
                {
                    break;
                }

                if (line.StartsWith("VIDEO_REQ|", StringComparison.OrdinalIgnoreCase))
                {
                    _ = Task.Run(() => videoRequestHandler(this, line));
                    continue;
                }

                responseQueue.Enqueue(line);
                responseSignal.Release();
            }
        }
        catch (ObjectDisposedException)
        {
        }
        catch
        {
        }
        finally
        {
            IsConnected = false;
            responseQueue.Enqueue("ERROR|LIGACAO_FECHADA");
            responseSignal.Release();
        }
    }

    private async Task<string?> WaitForProtocolResponseAsync(int timeoutSeconds)
    {
        bool received = await responseSignal.WaitAsync(TimeSpan.FromSeconds(timeoutSeconds));
        if (!received)
        {
            return null;
        }

        return responseQueue.TryDequeue(out string? response)
            ? response
            : null;
    }
}
