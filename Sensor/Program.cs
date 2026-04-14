using DataGenerator;
using System.Net.Sockets;
using System.Text;
using System.Threading;

const string GATEWAY_IP = "127.0.0.1";
const int GATEWAY_PORT = 5000;
const int HEARTBEAT_INTERVAL_MS = 10000;
const int BATTERY_DRAIN_INTERVAL_MS = 15000;
const int BATTERY_CHARGE_INTERVAL_MS = 5000;
const int LOW_BATTERY_THRESHOLD = 20;
const int HEARTBEAT_BATTERY_COST = 1;
const int DATA_BATTERY_COST = 3;
const int IDLE_BATTERY_COST = 1;
const int CHARGE_STEP = 10;
const string PROFILES_FOLDER = "profiles";

SensorConfig config = ReadStartupConfig();
string sensorId = config.SensorId;
string zona = config.Zona;
string tiposDados = config.TiposDados;
List<string> configuredDataTypes = tiposDados
    .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
    .Select(type => type.ToUpperInvariant())
    .ToList();

TcpClient? client = null;
StreamReader? reader = null;
StreamWriter? writer = null;

bool ligado = false;
bool modoManutencao = false;
bool heartbeatAtivo = false;
bool aCarregar = false;
bool lowBatteryAlertSent = false;
bool autoSendEnabled = false;
bool resumeAutoSendAfterCharging = false;
int heartbeatCount = 0;
int batteryLevel = 100;
int autoSendIntervalSeconds = 60;
int autoSendCycleCount = 0;
Task? chargingTask = null;

Lock batteryLock = new();
Lock autoSendLock = new();
SemaphoreSlim socketLock = new(1, 1);

try
{
    client = new TcpClient();
    await client.ConnectAsync(GATEWAY_IP, GATEWAY_PORT);

    NetworkStream stream = client.GetStream();
    reader = new StreamReader(stream, Encoding.UTF8);
    writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true };

    string connectMsg = $"CONNECT|{sensorId}|{tiposDados}|{zona}";
    await writer.WriteLineAsync(connectMsg);

    string? response = await reader.ReadLineAsync();
    Console.WriteLine($"[SENSOR] Resposta: {response}");

    if (response == "CONN_ACK|ACEITE")
    {
        ligado = true;
        modoManutencao = false;
        heartbeatAtivo = true;
        Console.WriteLine("[SENSOR] Ligação aceite em modo normal.");
        _ = Task.Run(HeartbeatLoop);
    }
    else if (response == "CONN_ACK|MANUTENCAO")
    {
        ligado = true;
        modoManutencao = true;
        heartbeatAtivo = false;
        Console.WriteLine("[SENSOR] Ligação aceite em modo manutenção.");
    }
    else
    {
        Console.WriteLine("[SENSOR] Ligação recusada. O programa vai terminar.");
        await FecharLigacaoLocal();
        return;
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
    Console.WriteLine($"Estado: {(aCarregar ? "a carregar" : modoManutencao ? "manutenção" : "normal")}");
    Console.WriteLine($"Envio automático: {(IsAutoSendEnabled() ? $"ativo ({GetAutoSendIntervalSeconds()}s)" : "desativado")}");
    Console.WriteLine($"Tipos configurados: {string.Join(", ", configuredDataTypes)}");
    Console.WriteLine("1 - Enviar medição manual");
    Console.WriteLine("2 - Colocar a carregar");
    Console.WriteLine("3 - Ver bateria");
    Console.WriteLine("4 - Ativar/desativar envio automático");
    Console.WriteLine("5 - Alterar intervalo do envio automático");
    Console.WriteLine("6 - Desligar");
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

    Console.Write("Valor: ");
    string valor = Console.ReadLine()?.Trim() ?? "0";

    await EnviarMedicaoAsync(tipo, valor, origemAutomatica: false);
}

async Task EnviarMedicaoAsync(string tipo, string valor, bool origemAutomatica)
{
    if (!ligado || writer == null || reader == null)
    {
        Console.WriteLine("O sensor não está ligado ao gateway.");
        return;
    }

    if (aCarregar)
    {
        Console.WriteLine("[SENSOR] Não é possível enviar medições enquanto a bateria está a carregar.");
        return;
    }

    if (modoManutencao && !origemAutomatica)
    {
        Console.WriteLine("[SENSOR] O sensor está em manutenção.");
        return;
    }

    if (GetBatteryLevel() <= 0)
    {
        Console.WriteLine("[SENSOR] Bateria esgotada. Coloque o sensor a carregar.");
        modoManutencao = true;
        heartbeatAtivo = false;
        SetAutoSendEnabled(false);
        return;
    }

    if (!configuredDataTypes.Contains(tipo, StringComparer.OrdinalIgnoreCase))
    {
        Console.WriteLine($"[SENSOR] O tipo {tipo} não está configurado neste sensor.");
        return;
    }

    string timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");

    bool shouldCheckLowBattery = false;

    await socketLock.WaitAsync();
    try
    {
        string dataMsg = $"DATA|{sensorId}|{timestamp}|{tipo}|{valor}";
        await writer.WriteLineAsync(dataMsg);

        string? response = await reader.ReadLineAsync();
        Console.WriteLine($"[SENSOR] Resposta: {response}");

        if (response == "DATA_ACK|SUCESSO")
        {
            ConsumeBattery(DATA_BATTERY_COST);
            shouldCheckLowBattery = true;
        }
    }
    finally
    {
        socketLock.Release();
    }

    if (shouldCheckLowBattery)
    {
        await CheckLowBatteryAsync();
    }
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

    Console.WriteLine(newState
        ? $"[SENSOR] Envio automático ativado com intervalo de {GetAutoSendIntervalSeconds()} segundos."
        : "[SENSOR] Envio automático desativado.");
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
}

async Task AutoSendLoop()
{
    while (true)
    {
        await Task.Delay(500);

        if (!ligado || !IsAutoSendEnabled() || aCarregar || modoManutencao)
        {
            continue;
        }

        int intervalSeconds = GetAutoSendIntervalSeconds();
        await Task.Delay(TimeSpan.FromSeconds(intervalSeconds));

        if (!ligado || !IsAutoSendEnabled() || aCarregar || modoManutencao)
        {
            continue;
        }

        autoSendCycleCount++;
        Console.WriteLine($"[AUTO] Início da ronda {autoSendCycleCount}.");

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
    }
}

Task IniciarCarregamento()
{
    if (!ligado || writer == null || reader == null)
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
    resumeAutoSendAfterCharging = IsAutoSendEnabled();
    SetAutoSendEnabled(false);

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

        Console.WriteLine($"[SENSOR] A carregar... bateria a {currentLevel}%.");

        if (completed)
        {
            aCarregar = false;
            modoManutencao = false;
            lowBatteryAlertSent = false;
            heartbeatAtivo = true;
            bool restoreAutoSend = resumeAutoSendAfterCharging;
            resumeAutoSendAfterCharging = false;

            Console.WriteLine("[SENSOR] Carregamento concluído. Sensor novamente ativo.");
            _ = Task.Run(() => TrySendNotificationAsync("CHARGING", "COMPLETO"));
            _ = Task.Run(HeartbeatLoop);
            if (restoreAutoSend)
            {
                SetAutoSendEnabled(true);
                Console.WriteLine($"[SENSOR] Envio automático restaurado com intervalo de {GetAutoSendIntervalSeconds()} segundos.");
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

            if (!heartbeatAtivo || !ligado || writer == null || reader == null || aCarregar)
            {
                continue;
            }

            if (GetBatteryLevel() <= 0)
            {
                heartbeatAtivo = false;
                modoManutencao = true;
                SetAutoSendEnabled(false);
                Console.WriteLine("[SENSOR] Heartbeat parado por bateria esgotada.");
                continue;
            }

            bool shouldCheckLowBattery = false;

            await socketLock.WaitAsync();
            try
            {
                string timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");
                string hbMsg = $"HEARTBEAT|{sensorId}|{timestamp}";
                await writer.WriteLineAsync(hbMsg);

                string? response = await reader.ReadLineAsync();
                heartbeatCount++;

                if (response == null || !response.StartsWith("ACK_HEARTBEAT|SUCESSO", StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine($"[SENSOR] Erro no heartbeat: {response}");
                }
                else
                {
                    ConsumeBattery(HEARTBEAT_BATTERY_COST);
                    shouldCheckLowBattery = true;

                    if (heartbeatCount % 10 == 0)
                    {
                        Console.WriteLine($"[SENSOR] {heartbeatCount} heartbeats enviados com sucesso.");
                    }
                }
            }
            finally
            {
                socketLock.Release();
            }

            if (shouldCheckLowBattery)
            {
                await CheckLowBatteryAsync();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[SENSOR] Erro no heartbeat: {ex.Message}");
            break;
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
        await CheckLowBatteryAsync();
    }
}

async Task CheckLowBatteryAsync()
{
    int currentBattery = GetBatteryLevel();

    if (currentBattery > LOW_BATTERY_THRESHOLD)
    {
        lowBatteryAlertSent = false;
        return;
    }

    if (lowBatteryAlertSent || aCarregar)
    {
        if (currentBattery <= 0)
        {
            modoManutencao = true;
            heartbeatAtivo = false;
            SetAutoSendEnabled(false);
            Console.WriteLine("[SENSOR] Bateria esgotada. O sensor precisa de carregamento.");
        }
        return;
    }

    modoManutencao = true;
    heartbeatAtivo = false;
    SetAutoSendEnabled(false);
    lowBatteryAlertSent = true;

    Console.WriteLine($"[SENSOR] Bateria baixa ({currentBattery}%). A notificar o gateway.");
    await SendNotificationAsync("LOW_BATTERY", $"{currentBattery}%");

    if (currentBattery <= 0)
    {
        Console.WriteLine("[SENSOR] Bateria esgotada. O sensor precisa de carregamento.");
    }
}

async Task SendNotificationAsync(string notificationType, string payload)
{
    if (!ligado || writer == null || reader == null)
    {
        return;
    }

    await socketLock.WaitAsync();
    try
    {
        string notifyMsg = $"NOTIFY|{sensorId}|{notificationType}|{payload}";
        await writer.WriteLineAsync(notifyMsg);

        string? response = await reader.ReadLineAsync();
        Console.WriteLine($"[SENSOR] Resposta: {response}");
    }
    finally
    {
        socketLock.Release();
    }
}

async Task TrySendNotificationAsync(string notificationType, string payload)
{
    try
    {
        using CancellationTokenSource cts = new(TimeSpan.FromSeconds(3));
        bool acquired = await socketLock.WaitAsync(0, cts.Token);
        if (!acquired)
        {
            Console.WriteLine($"[SENSOR] Não foi possível notificar {notificationType} de imediato.");
            return;
        }

        try
        {
            if (!ligado || writer == null || reader == null)
            {
                return;
            }

            string notifyMsg = $"NOTIFY|{sensorId}|{notificationType}|{payload}";
            await writer.WriteLineAsync(notifyMsg);
            string? response = await reader.ReadLineAsync();
            Console.WriteLine($"[SENSOR] Resposta: {response}");
        }
        finally
        {
            socketLock.Release();
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

        if (ligado && writer != null && reader != null)
        {
            await socketLock.WaitAsync();
            try
            {
                string discMsg = $"DISCONNECT|{sensorId}";
                Console.WriteLine($"[SENSOR] A enviar: {discMsg}");
                await writer.WriteLineAsync(discMsg);

                string? response = await reader.ReadLineAsync();
                Console.WriteLine($"[SENSOR] Resposta: {response}");
            }
            finally
            {
                socketLock.Release();
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

    try { reader?.Dispose(); } catch { }
    try { writer?.Dispose(); } catch { }
    try { client?.Dispose(); } catch { }

    reader = null;
    writer = null;
    client = null;

    await Task.CompletedTask;
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
