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
object lockBuffer = new object();
object lockCsv = new object();
List<string> _dataBuffer = new List<string>();

bool gatewayEmExecucao = true;

// --- INICIALIZAÇÃO ---
EnsureCsvExists();
TcpListener listener = new TcpListener(IPAddress.Any, GATEWAY_PORT);
listener.Start();

Console.WriteLine($"[GATEWAY {GATEWAY_ID}] Ativo na porta {GATEWAY_PORT}...");
Console.WriteLine("Comandos: Escreva 'send' para forçar o envio do lote atual.");

// Thread para envio automático por tempo (Tarefa 2.2)
Thread batchTimerThread = new Thread(BatchTimerLoop)
{
    Name = "BatchTimer",
    IsBackground = false
};
batchTimerThread.Start();

// Thread para comandos manuais na consola
Thread commandThread = new Thread(ManualCommandLoop)
{
    Name = "CommandListener",
    IsBackground = false
};
commandThread.Start();

// Loop principal - Aceitar conexões de sensores
Console.WriteLine("[GATEWAY] À espera de sensores...");
while (gatewayEmExecucao)
{
    try
    {
        TcpClient sensorClient = listener.AcceptTcpClient();
        
        // Criar thread para cada sensor conectado
        Thread sensorThread = new Thread(HandleSensor)
        {
            Name = $"Sensor-{DateTime.Now:HHmmss}",
            IsBackground = true
        };
        sensorThread.Start(sensorClient);
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[GATEWAY] Erro ao aceitar conexão: {ex.Message}");
    }
}

return;

void HandleSensor(object? clientObj)
{
    if (clientObj is not TcpClient client)
        return;

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
                string? line = reader.ReadLine();
                if (line == null) break;

                string[] parts = line.Split('|');
                if (parts.Length == 0) continue;

                string command = parts[0].ToUpper();

                switch (command)
                {
                    case "CONNECT": // --- TAREFA 2.1: VALIDAÇÃO RIGOROSA ---
                        if (parts.Length < 4) break;
                        string id = parts[1];
                        string tiposSolicitadosRaw = parts[2];
                        string zona = parts[3];

                        var info = FindSensor(id);

                        if (info != null && info.Estado != "desativado" && info.Zona.Equals(zona, StringComparison.OrdinalIgnoreCase))
                        {
                            // 1. Criar lista do que o sensor enviou agora
                            var tiposSolicitados = tiposSolicitadosRaw.Split(',')
                                                                     .Select(t => t.Trim())
                                                                     .ToList();

                            // 2. VALIDAR: Todos os tipos que o sensor enviou existem na lista do CSV?
                            // Ex: Se o sensor mandou "temperatura" mas o CSV diz "TEMP", isto vai dar FALSE.
                            bool todosOsTiposSaoValidos = tiposSolicitados.All(ts =>
                                info.TiposDados.Any(csvT => csvT.Equals(ts, StringComparison.OrdinalIgnoreCase)));

                            if (todosOsTiposSaoValidos)
                            {
                                currentSensorId = id;
                                // Guardamos os tipos autorizados (usamos os do sensor porque já confirmámos que são válidos)
                                currentTipos = new HashSet<string>(tiposSolicitados, StringComparer.OrdinalIgnoreCase);
                                isConnected = true;

                                UpdateSensorEntry(id, info.Estado);

                                string resp = (info.Estado == "manutencao") ? "CONN_ACK|MANUTENCAO" : "CONN_ACK|ACEITE";
                                writer.WriteLine(resp);
                                writer.Flush();
                                Console.WriteLine($"[ACEITE] Sensor {id} conectado. Tipos: {string.Join(", ", currentTipos)}");
                            }
                            else
                            {
                                // Se o tipo for "temperatura" em vez de "TEMP", ele cai aqui:
                                writer.WriteLine("CONN_ACK|RECUSADO");
                                writer.Flush();
                                Console.WriteLine($"[RECUSADO] Sensor {id} tentou tipos não autorizados: {tiposSolicitadosRaw}");
                            }
                        }
                        else
                        {
                            writer.WriteLine("CONN_ACK|RECUSADO");
                            writer.Flush();
                            Console.WriteLine($"[RECUSADO] Sensor {id} falhou na validação de ID, Estado ou Zona.");
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

                                lock (lockBuffer)
                                {
                                    _dataBuffer.Add(line);
                                    if (_dataBuffer.Count >= MAX_BATCH_SIZE)
                                    {
                                        // Forçar envio em thread dedicada
                                        Thread envioThread = new Thread(ForwardBatchToServer)
                                        {
                                            Name = $"EnvioBatch-{DateTime.Now:HHmmss}",
                                            IsBackground = true
                                        };
                                        envioThread.Start();
                                    }
                                }

                                UpdateSensorEntry(currentSensorId, "ativo");
                                writer.WriteLine("DATA_ACK|SUCESSO");
                                writer.Flush();
                            }
                            else
                            {
                                writer.WriteLine("DATA_ACK|ERRO|TIPO_NAO_SUPORTADO");
                                writer.Flush();
                            }
                        }
                        break;

                    case "LOTE":
                        if (isConnected && currentSensorId != null && parts.Length >= 4)
                        {
                            try
                            {
                                int qtd = int.Parse(parts[2]);
                                string medicoesBrutas = string.Join("|", parts.Skip(3)); // Recriar dados em caso de split extra
                                var medicoes = medicoesBrutas.Split('#');

                                Console.WriteLine($"[LOTE] Sensor {currentSensorId} enviou {qtd} medições");

                                foreach (var med in medicoes)
                                {
                                    if (string.IsNullOrWhiteSpace(med)) continue;

                                    var medPartes = med.Split('|');
                                    if (medPartes.Length >= 2)
                                    {
                                        string tipo = medPartes[0].Trim();
                                        
                                        if (currentTipos.Contains(tipo))
                                        {
                                            LogDataLocally($"DATA|{currentSensorId}|{medPartes[2]}|{tipo}|{medPartes[1]}");

                                            lock (lockBuffer)
                                            {
                                                _dataBuffer.Add($"DATA|{currentSensorId}|{medPartes[2]}|{tipo}|{medPartes[1]}");
                                            }
                                        }
                                    }
                                }

                                UpdateSensorEntry(currentSensorId, "ativo");
                                writer.WriteLine("LOTE_ACK|SUCESSO");
                                writer.Flush();
                                Console.WriteLine($"[LOTE] ✓ Lote processado com sucesso");

                                // Verificar se precisa enviar ao servidor
                                lock (lockBuffer)
                                {
                                    if (_dataBuffer.Count >= MAX_BATCH_SIZE)
                                    {
                                        Thread envioThread = new Thread(ForwardBatchToServer)
                                        {
                                            Name = $"EnvioBatch-{DateTime.Now:HHmmss}",
                                            IsBackground = true
                                        };
                                        envioThread.Start();
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"[LOTE] ✗ Erro ao processar lote: {ex.Message}");
                                writer.WriteLine("LOTE_ACK|ERRO");
                                writer.Flush();
                            }
                        }
                        break;

                    case "HEARTBEAT":
                        if (isConnected && currentSensorId != null)
                        {
                            UpdateSensorEntry(currentSensorId, "ativo");
                            writer.WriteLine("ACK_HEARTBEAT|SUCESSO");
                            writer.Flush();
                        }
                        break;

                    case "NOTIFY":
                        if (parts.Length >= 4 && parts[3] == "LOW_BATTERY")
                        {
                            Console.WriteLine($"[ALERTA] Bateria Baixa no Sensor {parts[1]}: {parts[2]}");
                            UpdateSensorEntry(parts[1], "manutencao");
                        }
                        writer.WriteLine("ACK_NOTIFY|SUCESSO");
                        writer.Flush();
                        break;

                    case "DISCONNECT": // --- CORREÇÃO: RECEBER DESLIGAMENTO ---
                        if (isConnected && currentSensorId != null)
                        {
                            Console.WriteLine($"[DESLIGAR] Sensor {currentSensorId} solicitou o encerramento da ligação.");
                            writer.WriteLine("ACK_DISCONNECT|SUCESSO");
                            writer.Flush();
                            return; // Encerra a comunicação com este sensor
                        }
                        break;
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[GATEWAY] Erro na ligação: {ex.Message}");
        }
    }
}

void ForwardBatchToServer()
{
    lock (lockBuffer)
    {
        if (_dataBuffer.Count == 0) return;

        List<string> batch = new List<string>(_dataBuffer);
        _dataBuffer.Clear();

        string msg = $"DATA_BATCH|{GATEWAY_ID}|{batch.Count}|{string.Join("#", batch)}";

        try
        {
            TcpClient server = new TcpClient();
            server.Connect(SERVER_IP, SERVER_PORT);

            using (NetworkStream stream = server.GetStream())
            using (StreamWriter sw = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true })
            using (StreamReader sr = new StreamReader(stream, Encoding.UTF8))
            {
                sw.WriteLine(msg);
                sw.Flush();

                string? ack = sr.ReadLine();
                Console.WriteLine($"[BATCH] Lote de {batch.Count} enviado. Resposta do servidor: {ack}");
            }
            server.Close();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ERRO] Servidor offline. Dados mantidos no histórico local. {ex.Message}");
        }
    }
}

void BatchTimerLoop()
{
    while (gatewayEmExecucao)
    {
        try
        {
            Thread.Sleep(BATCH_TIMEOUT_MS);
            ForwardBatchToServer();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[BATCH] Erro no timer: {ex.Message}");
        }
    }
}

void ManualCommandLoop()
{
    while (gatewayEmExecucao)
    {
        try
        {
            string? cmd = Console.ReadLine();
            if (cmd?.ToLower() == "send")
            {
                Console.WriteLine("[MANUAL] A forçar envio do lote...");
                ForwardBatchToServer();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[COMMAND] Erro: {ex.Message}");
        }
    }
}

void UpdateSensorEntry(string id, string estado)
{
    lock (lockCsv)
    {
        try
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
        catch (Exception ex)
        {
            Console.WriteLine($"[CSV] Erro ao atualizar: {ex.Message}");
        }
    }
}

void LogDataLocally(string data)
{
    try
    {
        File.AppendAllText(HISTORY_FILE, $"[{DateTime.Now}] {data}{Environment.NewLine}");
    }
    catch { }
}

SensorInfo? FindSensor(string sensorId)
{
    try
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
    }
    catch { }
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