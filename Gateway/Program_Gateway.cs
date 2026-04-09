using System.Net;
using System.Net.Sockets;
using System.Text;

// --- CONFIGURAÇÕES DO GATEWAY ---
const int GATEWAY_PORT = 5000;
const string SERVER_IP = "127.0.0.1";
const int SERVER_PORT = 6000;
const string GATEWAY_ID = "GW01";
const string CSV_FILE = "sensors.csv";
const string HISTORY_FILE = "gateway_history.txt";

// --- CONFIGURAÇÕES DE LOTE (Tarefa 2.2) ---
const int MAX_BATCH_SIZE = 5;
const int BATCH_TIMEOUT_MS = 300000;
List<string> _dataBuffer = new List<string>();

// --- INICIALIZAÇÃO ---
EnsureCsvExists();
TcpListener listener = new TcpListener(IPAddress.Any, GATEWAY_PORT);
listener.Start();

Console.WriteLine($"[GATEWAY {GATEWAY_ID}] Ativo na porta {GATEWAY_PORT}...");
Console.WriteLine("Comandos: Escreva 'send' para forçar o envio do lote atual.");

// Loop para envio automático por tempo (Tarefa 2.2)
_ = Task.Run(BatchTimerLoop);

// Loop para comandos manuais na consola
_ = Task.Run(ManualCommandLoop);

while (true)
{
    TcpClient sensorClient = await listener.AcceptTcpClientAsync();
    _ = Task.Run(() => HandleSensorAsync(sensorClient));
}

async Task HandleSensorAsync(TcpClient client)
{
    using (client)
    using (NetworkStream stream = client.GetStream())
    using (StreamReader reader = new StreamReader(stream, Encoding.UTF8))
    using (StreamWriter writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true })
    {
        string? currentSensorId = null;
        bool isConnected = false;
        HashSet<string> currentTipos = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        try
        {
            while (true)
            {
                string? line = await reader.ReadLineAsync();
                if (line == null) break;

                string[] parts = line.Split('|');
                if (parts.Length == 0) continue;

                string command = parts[0].ToUpper();

                switch (command)
                {
                    case "CONNECT": // --- TAREFA 2.1: VALIDAÇÃO ---
                        if (parts.Length < 4) break;
                        string id = parts[1];
                        string tiposStr = parts[2];
                        string zona = parts[3];

                        var info = FindSensor(id);

                        if (info != null && info.Estado != "desativado" && info.Zona.Equals(zona, StringComparison.OrdinalIgnoreCase))
                        {
                            currentSensorId = id;
                            currentTipos = new HashSet<string>(tiposStr.Split(','), StringComparer.OrdinalIgnoreCase);
                            isConnected = true;

                            UpdateSensorEntry(id, info.Estado);

                            string resp = (info.Estado == "manutencao") ? "CONN_ACK|MANUTENCAO" : "CONN_ACK|ACEITE";
                            await writer.WriteLineAsync(resp);
                            Console.WriteLine($"[LIGAR] Sensor {id} conectado com sucesso.");
                        }
                        else
                        {
                            await writer.WriteLineAsync("CONN_ACK|RECUSADO");
                        }
                        break;

                    case "DATA": // --- TAREFA 2.2 e 2.3: AGREGAÇÃO E REGISTO ---
                        if (isConnected && currentSensorId != null && parts.Length >= 5)
                        {
                            string tipoDado = parts[3];

                            if (currentTipos.Contains(tipoDado))
                            {
                                Console.WriteLine($"[RECEBIDO] Sensor {currentSensorId} enviou {tipoDado}: {parts[4]} (Guardado em buffer)");

                                LogDataLocally(line);

                                _dataBuffer.Add(line);
                                if (_dataBuffer.Count >= MAX_BATCH_SIZE)
                                {
                                    _ = ForwardBatchToServer();
                                }

                                UpdateSensorEntry(currentSensorId, "ativo");
                                await writer.WriteLineAsync("DATA_ACK|SUCESSO");
                            }
                            else
                            {
                                await writer.WriteLineAsync("DATA_ACK|ERRO|TIPO_NAO_SUPORTADO");
                            }
                        }
                        break;

                    case "HEARTBEAT":
                        if (isConnected && currentSensorId != null)
                        {
                            UpdateSensorEntry(currentSensorId, "ativo");
                            await writer.WriteLineAsync("ACK_HEARTBEAT|SUCESSO");
                        }
                        break;

                    case "NOTIFY":
                        if (parts.Length >= 4 && parts[2] == "LOW_BATTERY")
                        {
                            Console.WriteLine($"[ALERTA] Bateria Baixa no Sensor {parts[1]}: {parts[3]}");
                            UpdateSensorEntry(parts[1], "manutencao");
                        }
                        await writer.WriteLineAsync("ACK_NOTIFY|SUCESSO");
                        break;

                    case "DISCONNECT": // --- CORREÇÃO: RECEBER DESLIGAMENTO ---
                        if (isConnected && currentSensorId != null)
                        {
                            Console.WriteLine($"[DESLIGAR] Sensor {currentSensorId} solicitou o encerramento da ligação.");

                            // Conforme o protocolo: atualizar estado para desativado ao fechar
                            UpdateSensorEntry(currentSensorId, "desativado");

                            await writer.WriteLineAsync("ACK_DISCONNECT|SUCESSO");
                            return; // Encerra a comunicação com este sensor
                        }
                        break;
                }
            }
        }
        catch (Exception ex)
        {
            // Agora a variável 'ex' é usada aqui:
            Console.WriteLine($"[GATEWAY] Erro na ligação: {ex.Message}");
        }
    }
}

async Task ForwardBatchToServer()
{
    if (_dataBuffer.Count == 0) return;

    List<string> batch = new List<string>(_dataBuffer);
    _dataBuffer.Clear();

    string msg = $"DATA_BATCH|{GATEWAY_ID}|{batch.Count}|{string.Join("#", batch)}";

    try
    {
        using TcpClient server = new TcpClient();
        await server.ConnectAsync(SERVER_IP, SERVER_PORT);

        using NetworkStream stream = server.GetStream();
        using StreamWriter sw = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true };
        using StreamReader sr = new StreamReader(stream, Encoding.UTF8);

        await sw.WriteLineAsync(msg);

        string? ack = await sr.ReadLineAsync();
        Console.WriteLine($"[BATCH] Lote de {batch.Count} enviado. Resposta do servidor: {ack}");
    }
    catch
    {
        Console.WriteLine("[ERRO] Servidor offline. Dados mantidos no histórico local.");
    }
}

async Task BatchTimerLoop()
{
    while (true)
    {
        await Task.Delay(BATCH_TIMEOUT_MS);
        await ForwardBatchToServer();
    }
}

async Task ManualCommandLoop()
{
    while (true)
    {
        string? cmd = Console.ReadLine();
        if (cmd?.ToLower() == "send")
        {
            Console.WriteLine("[MANUAL] A forçar envio do lote...");
            await ForwardBatchToServer();
        }
    }
}

void UpdateSensorEntry(string id, string estado)
{
    var lines = File.ReadAllLines(CSV_FILE).ToList();
    for (int i = 1; i < lines.Count; i++)
    {
        var p = lines[i].Split(';');
        if (p[0].Trim().Equals(id, StringComparison.OrdinalIgnoreCase))
        {
            p[1] = estado;
            p[4] = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");
            lines[i] = string.Join(';', p);
            break;
        }
    }
    File.WriteAllLines(CSV_FILE, lines);
}

void LogDataLocally(string data)
{
    File.AppendAllText(HISTORY_FILE, $"[{DateTime.Now}] {data}{Environment.NewLine}");
}

SensorInfo? FindSensor(string sensorId)
{
    if (!File.Exists(CSV_FILE)) return null;
    var lines = File.ReadAllLines(CSV_FILE).Skip(1);
    foreach (var line in lines)
    {
        var p = line.Split(';');
        if (p.Length >= 5 && p[0].Trim().Equals(sensorId, StringComparison.OrdinalIgnoreCase))
        {
            return new SensorInfo
            {
                SensorId = p[0].Trim(),
                Estado = p[1].Trim().ToLower(),
                Zona = p[2].Trim(),
                TiposDados = p[3].Split(',').Select(t => t.Trim()).ToList()
            };
        }
    }
    return null;
}

void EnsureCsvExists()
{
    if (!File.Exists(CSV_FILE))
    {
        File.WriteAllLines(CSV_FILE, new[] {
            "sensor_id;estado;zona;tipos_dados;last_sync",
            "S101;ativo;ZONA_PARQUE;TEMP,HUM,RUIDO;-",
            "S102;ativo;ZONA_ESCOLAR;TEMP,PM2.5,RUIDO;-",
            "S103;desativado;ZONA_CENTRO;TEMP,HUM;-",
            "S104;ativo;ZONA_PASSEIO;TEMP,HUM,RUIDO;-",
            "S105;ativo;ZONA_ESCOLAR;TEMP,HUM,RUIDO;-"
        });
    }
}

class SensorInfo
{
    public string SensorId { get; set; } = "";
    public string Estado { get; set; } = "";
    public string Zona { get; set; } = "";
    public List<string> TiposDados { get; set; } = new();
}