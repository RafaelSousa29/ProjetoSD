using System.Net;
using System.Net.Sockets;
using System.Text;

const int GATEWAY_PORT = 5000;
const string SERVER_IP = "127.0.0.1";
const int SERVER_PORT = 6000;
const string GATEWAY_ID = "GW01";
const string CSV_FILE = "sensors.csv";

EnsureCsvExists();

TcpListener listener = new TcpListener(IPAddress.Any, GATEWAY_PORT);
listener.Start();

Console.WriteLine($"[GATEWAY] À escuta na porta {GATEWAY_PORT}...");

while (true)
{
    TcpClient sensorClient = await listener.AcceptTcpClientAsync();
    Console.WriteLine("[GATEWAY] Sensor ligado.");
    _ = Task.Run(() => HandleSensorAsync(sensorClient));
}

static async Task HandleSensorAsync(TcpClient sensorClient)
{
    using (sensorClient)
    using (NetworkStream sensorStream = sensorClient.GetStream())
    using (StreamReader sensorReader = new StreamReader(sensorStream, Encoding.UTF8))
    using (StreamWriter sensorWriter = new StreamWriter(sensorStream, Encoding.UTF8) { AutoFlush = true })
    {
        string? currentSensorId = null;
        string? currentZona = null;
        HashSet<string> currentTipos = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        string currentEstado = "";
        bool connected = false;

        try
        {
            while (true)
            {
                string? line = await sensorReader.ReadLineAsync();
                if (line == null) break;

                Console.WriteLine($"[GATEWAY] Recebido: {line}");
                string[] parts = line.Split('|');

                if (parts.Length == 0)
                    continue;

                string command = parts[0].Trim().ToUpperInvariant();

                switch (command)
                {
                    case "CONNECT":
                        {
                            if (parts.Length < 4)
                            {
                                await sensorWriter.WriteLineAsync("CONN_ACK|RECUSADO|FORMATO_INVALIDO");
                                break;
                            }

                            string sensorId = parts[1].Trim();
                            string tipos = parts[2].Trim();
                            string zona = parts[3].Trim();

                            SensorInfo? sensorInfo = FindSensor(sensorId);

                            if (sensorInfo == null)
                            {
                                await sensorWriter.WriteLineAsync("CONN_ACK|RECUSADO|SENSOR_NAO_REGISTADO");
                                break;
                            }

                            if (!sensorInfo.Zona.Equals(zona, StringComparison.OrdinalIgnoreCase))
                            {
                                await sensorWriter.WriteLineAsync("CONN_ACK|RECUSADO|ZONA_INVALIDA");
                                break;
                            }

                            string[] requestedTypes = tipos
                                .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

                            bool unsupported = requestedTypes.Any(t =>
                                !sensorInfo.TiposDados.Contains(t, StringComparer.OrdinalIgnoreCase));

                            if (unsupported)
                            {
                                await sensorWriter.WriteLineAsync("CONN_ACK|RECUSADO|TIPO_NAO_SUPORTADO");
                                break;
                            }

                            if (sensorInfo.Estado.Equals("desativado", StringComparison.OrdinalIgnoreCase))
                            {
                                await sensorWriter.WriteLineAsync("CONN_ACK|RECUSADO|ESTADO_DESATIVADO");
                                break;
                            }

                            currentSensorId = sensorId;
                            currentZona = zona;
                            currentTipos = new HashSet<string>(requestedTypes, StringComparer.OrdinalIgnoreCase);
                            currentEstado = sensorInfo.Estado.ToLowerInvariant();
                            connected = true;

                            UpdateLastSync(sensorId, DateTime.Now);

                            if (sensorInfo.Estado.Equals("ativo", StringComparison.OrdinalIgnoreCase))
                            {
                                await ForwardToServerAsync($"FORWARD_CONNECT|{GATEWAY_ID}|{sensorId}|{zona}|{tipos}|ATIVO");
                                await sensorWriter.WriteLineAsync("CONN_ACK|ACEITE");
                            }
                            else if (sensorInfo.Estado.Equals("manutencao", StringComparison.OrdinalIgnoreCase))
                            {
                                await ForwardToServerAsync($"FORWARD_CONNECT|{GATEWAY_ID}|{sensorId}|{zona}|{tipos}|MANUTENCAO");
                                await sensorWriter.WriteLineAsync("CONN_ACK|MANUTENCAO");
                            }
                            else
                            {
                                await sensorWriter.WriteLineAsync("CONN_ACK|RECUSADO|ESTADO_INVALIDO");
                            }

                            break;
                        }

                    case "DATA":
                        {
                            if (!connected || currentSensorId == null || currentZona == null)
                            {
                                await sensorWriter.WriteLineAsync("DATA_ACK|ERRO|NAO_LIGADO");
                                break;
                            }

                            if (parts.Length < 5)
                            {
                                await sensorWriter.WriteLineAsync("DATA_ACK|ERRO|FORMATO_INVALIDO");
                                break;
                            }

                            string sensorId = parts[1].Trim();
                            string timestamp = parts[2].Trim();
                            string tipo = parts[3].Trim();
                            string valor = parts[4].Trim();

                            if (!sensorId.Equals(currentSensorId, StringComparison.OrdinalIgnoreCase))
                            {
                                await sensorWriter.WriteLineAsync("DATA_ACK|ERRO|SENSOR_ID_INVALIDO");
                                break;
                            }

                            if (!currentTipos.Contains(tipo))
                            {
                                await sensorWriter.WriteLineAsync("DATA_ACK|ERRO|TIPO_INVALIDO");
                                break;
                            }

                            UpdateLastSync(sensorId, DateTime.Now);

                            await ForwardToServerAsync(
                                $"FORWARD_DATA|{GATEWAY_ID}|{sensorId}|{timestamp}|{tipo}|{valor}|{currentZona}|{currentEstado.ToUpper()}");

                            await sensorWriter.WriteLineAsync("DATA_ACK|SUCESSO");
                            break;
                        }

                    case "HEARTBEAT":
                        {
                            if (!connected || currentSensorId == null)
                            {
                                await sensorWriter.WriteLineAsync("ACK_HEARTBEAT|ERRO|NAO_LIGADO");
                                break;
                            }

                            if (currentEstado.Equals("manutencao", StringComparison.OrdinalIgnoreCase))
                            {
                                await sensorWriter.WriteLineAsync("ACK_HEARTBEAT|MANUTENCAO");
                                break;
                            }

                            UpdateLastSync(currentSensorId, DateTime.Now);
                            await sensorWriter.WriteLineAsync("ACK_HEARTBEAT|SUCESSO");
                            break;
                        }

                    case "DISCONNECT":
                        {
                            if (!connected || currentSensorId == null)
                            {
                                await sensorWriter.WriteLineAsync("ACK_DISCONNECT|ERRO|NAO_LIGADO");
                                break;
                            }

                            await ForwardToServerAsync($"FORWARD_DISCONNECT|{GATEWAY_ID}|{currentSensorId}|{currentEstado.ToUpper()}");
                            await sensorWriter.WriteLineAsync("ACK_DISCONNECT|SUCESSO");
                            return;
                        }

                    default:
                        await sensorWriter.WriteLineAsync("ERRO|COMANDO_DESCONHECIDO");
                        break;
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[GATEWAY] Erro: {ex.Message}");
        }
    }

    Console.WriteLine("[GATEWAY] Sensor desligado.");
}

static async Task ForwardToServerAsync(string message)
{
    using TcpClient serverClient = new TcpClient();
    await serverClient.ConnectAsync(SERVER_IP, SERVER_PORT);

    using NetworkStream stream = serverClient.GetStream();
    using StreamReader reader = new StreamReader(stream, Encoding.UTF8);
    using StreamWriter writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true };

    await writer.WriteLineAsync(message);
    string? ack = await reader.ReadLineAsync();

    Console.WriteLine($"[GATEWAY] ACK do servidor: {ack}");
}

static SensorInfo? FindSensor(string sensorId)
{
    IEnumerable<string> lines = File.ReadAllLines(CSV_FILE).Skip(1);

    foreach (string line in lines)
    {
        string[] parts = line.Split(';');
        if (parts.Length < 5) continue;

        if (parts[0].Trim().Equals(sensorId, StringComparison.OrdinalIgnoreCase))
        {
            return new SensorInfo
            {
                SensorId = parts[0].Trim(),
                Estado = parts[1].Trim(),
                Zona = parts[2].Trim(),
                TiposDados = parts[3]
                    .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                    .ToList(),
                LastSync = parts[4].Trim()
            };
        }
    }

    return null;
}

static void UpdateLastSync(string sensorId, DateTime timestamp)
{
    List<string> lines = File.ReadAllLines(CSV_FILE).ToList();

    for (int i = 1; i < lines.Count; i++)
    {
        List<string> parts = lines[i].Split(';').ToList();
        if (parts.Count < 5) continue;

        if (parts[0].Trim().Equals(sensorId, StringComparison.OrdinalIgnoreCase))
        {
            parts[4] = timestamp.ToString("yyyy-MM-ddTHH:mm:ss");
            lines[i] = string.Join(';', parts);
            break;
        }
    }

    File.WriteAllLines(CSV_FILE, lines);
}

static void EnsureCsvExists()
{
    if (!File.Exists(CSV_FILE))
    {
        File.WriteAllLines(CSV_FILE, new[]
        {
            "sensor_id;estado;zona;tipos_dados;last_sync",
            "S101;ativo;ZONA_PARQUE;TEMP,RUIDO,HUM;-",
            "S102;manutencao;ZONA_ESCOLAR;TEMP,PM2.5,RUIDO;-",
            "S103;desativado;ZONA_CENTRO;TEMP,HUM;-"
        });
    }
}

class SensorInfo
{
    public string SensorId { get; set; } = "";
    public string Estado { get; set; } = "";
    public string Zona { get; set; } = "";
    public List<string> TiposDados { get; set; } = new();
    public string LastSync { get; set; } = "";
}