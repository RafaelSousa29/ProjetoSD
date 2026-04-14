using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

const int GATEWAY_PORT = 5000;
const string SERVER_IP = "127.0.0.1";
const int SERVER_PORT = 6000;
const string GATEWAY_ID = "GW01";
const string GATEWAY_CONFIG_FILE = "gateway_config.json";
const string ESTADO_DIR = "estado_sensores";
const string PEDIDOS_PENDENTES_FILE = "pedidos_sensores_pendentes.json";
const string HISTORY_FILE = "gateway_history.txt";

const int MAX_BATCH_SIZE = 5;
const int BATCH_TIMEOUT_MS = 300000;
const int HEARTBEAT_TIMEOUT_SECONDS = 60;

object lockBuffer = new object();
object lockEstado = new object();
object lockHistorico = new object();
object lockConfig = new object();
object lockPedidos = new object();

List<string> _dataBuffer = new List<string>();
Dictionary<string, SensorEstado> estadoSensores = new Dictionary<string, SensorEstado>();
List<PedidoSensor> pedidosPendentes = new List<PedidoSensor>();
bool gatewayEmExecucao = true;

// Carregar configuração
var gatewayConfig = CarregarGatewayConfig();

if (gatewayConfig == null)
{
    Console.WriteLine("[GATEWAY] ✗ Erro ao carregar configuração do gateway!");
    return;
}

// Carregar pedidos pendentes
CarregarPedidosPendentes();

// Criar pasta de estado
if (!Directory.Exists(ESTADO_DIR))
{
    Directory.CreateDirectory(ESTADO_DIR);
    Console.WriteLine($"[GATEWAY] ✓ Pasta '{ESTADO_DIR}' criada.");
}

// Inicializar estado dos sensores registados
foreach (var sensor in gatewayConfig.SensoresRegistados)
{
    estadoSensores[sensor.SensorId] = new SensorEstado
    {
        SensorId = sensor.SensorId,
        Zona = sensor.Zona,
        TiposDados = sensor.TiposDados.Split(',').Select(t => t.Trim()).ToList(),
        Estado = "offline",
        UltimoHeartbeat = DateTime.Now,
        UltimoSync = DateTime.MinValue
    };
    CarregarEstadosPersistidos();
    GuardarEstadoSensor(sensor.SensorId);
}

Console.WriteLine($"[GATEWAY {GATEWAY_ID}] ✓ {gatewayConfig.SensoresRegistados.Length} sensores registados");
Console.WriteLine($"[GATEWAY {GATEWAY_ID}] Ativo na porta {GATEWAY_PORT}...");
Console.WriteLine("Comandos: 'send' envio lote | 'status' estado sensores | 'pedidos' ver pedidos | 'aceitar S101' aprovar sensor | 'rejeitar S101' rejeitar");

// Thread para envio automático
Thread batchTimerThread = new Thread(BatchTimerLoop)
{
    Name = "BatchTimer",
    IsBackground = false
};
batchTimerThread.Start();

// Thread para monitorizar heartbeats
Thread heartbeatMonitorThread = new Thread(MonitorizarHeartbeats)
{
    Name = "HeartbeatMonitor",
    IsBackground = false
};
heartbeatMonitorThread.Start();

// Thread para comandos manuais
Thread commandThread = new Thread(ManualCommandLoop)
{
    Name = "CommandListener",
    IsBackground = false
};
commandThread.Start();

// TCP Listener
TcpListener listener = new TcpListener(IPAddress.Any, GATEWAY_PORT);
listener.Start();

Console.WriteLine("[GATEWAY] À espera de sensores...\n");

while (gatewayEmExecucao)
{
    try
    {
        TcpClient sensorClient = listener.AcceptTcpClient();
        
        Thread sensorThread = new Thread(HandleSensor)
        {
            Name = $"Sensor-{DateTime.Now:HHmmss}",
            IsBackground = true
        };
        sensorThread.Start(sensorClient);
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[GATEWAY] ✗ Erro ao aceitar conexão: {ex.Message}");
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
                    case "CONNECT":
                        if (parts.Length < 4) break;
                        
                        string sensorId = parts[1];
                        string tiposSolicitadosRaw = parts[2];  
                        string zona = parts[3];

                        // ✨ NOVO: Verificar se sensor está registado
                        if (!estadoSensores.ContainsKey(sensorId))
                        {
                            Console.WriteLine($"[GATEWAY] ❓ Sensor {sensorId} não registado - Pedido em análise...");
                            
                            // Adicionar aos pedidos pendentes
                            var pedido = new PedidoSensor
                            {
                                SensorId = sensorId,
                                Zona = zona,
                                TiposDados = tiposSolicitadosRaw,
                                DataPedido = DateTime.Now,
                                Status = "pendente"
                            };

                            lock (lockPedidos)
                            {
                                // Verificar se já existe pedido
                                if (!pedidosPendentes.Any(p => p.SensorId == sensorId && p.Status == "pendente"))
                                {
                                    pedidosPendentes.Add(pedido);
                                    GuardarPedidosPendentes();
                                    Console.WriteLine($"[GATEWAY] 📝 Pedido do sensor {sensorId} registado");
                                    Console.WriteLine($"[GATEWAY] ℹ️  Use 'aceitar {sensorId}' ou 'rejeitar {sensorId}' para processar");
                                }
                            }

                            writer.WriteLine("CONN_ACK|PENDENTE|SENSOR_EM_APROVACAO");
                            writer.Flush();
                            return;
                        }

                        var estadoSensor = estadoSensores[sensorId];

                        // Validar zona
                        if (!estadoSensor.Zona.Equals(zona, StringComparison.OrdinalIgnoreCase))
                        {
                            Console.WriteLine($"[GATEWAY] ✗ Sensor {sensorId} zona inválida: {zona}");
                            writer.WriteLine("CONN_ACK|RECUSADO|ZONA_INVALIDA");
                            writer.Flush();
                            return;
                        }

                        // Validar tipos de dados
                        var tiposSolicitados = tiposSolicitadosRaw.Split(',')
                            .Select(t => t.Trim().ToUpper())
                            .ToList();

                        bool todosOsTiposSaoValidos = tiposSolicitados.All(ts =>
                            estadoSensor.TiposDados.Contains(ts));

                        if (!todosOsTiposSaoValidos)
                        {
                            Console.WriteLine($"[GATEWAY] ✗ Sensor {sensorId} tipos não autorizados: {tiposSolicitadosRaw}");
                            writer.WriteLine("CONN_ACK|RECUSADO|TIPOS_NAO_SUPORTADOS");
                            writer.Flush();
                            return;
                        }

                        // Sensor aceite!
                        currentSensorId = sensorId;
                        currentTipos = new HashSet<string>(tiposSolicitados, StringComparer.OrdinalIgnoreCase);
                        isConnected = true;

                        lock (lockEstado)
                        {
                            estadoSensor.Estado = "ativo";
                            estadoSensor.UltimoHeartbeat = DateTime.Now;
                            CarregarEstadosPersistidos();
                            GuardarEstadoSensor(sensorId);
                        }

                        writer.WriteLine("CONN_ACK|ACEITE");
                        writer.Flush();
                        Console.WriteLine($"[GATEWAY] ✓ Sensor {sensorId} conectado - Tipos: {string.Join(", ", currentTipos)}");
                        break;

                    case "DATA":
                        if (!isConnected || currentSensorId == null || parts.Length < 5)
                        {
                            writer.WriteLine("DATA_ACK|ERRO|NAO_CONECTADO");
                            writer.Flush();
                            break;
                        }

                        string tipoDado = parts[3].ToUpper();

                        if (!currentTipos.Contains(tipoDado))
                        {
                            writer.WriteLine("DATA_ACK|ERRO|TIPO_NAO_SUPORTADO");
                            writer.Flush();
                            break;
                        }

                        GuardarHistorico(line);

                        lock (lockBuffer)
                        {
                            _dataBuffer.Add(line);
                            Console.WriteLine($"[GATEWAY] 📊 {currentSensorId} - {tipoDado}: {parts[4]}");

                            if (_dataBuffer.Count >= MAX_BATCH_SIZE)
                            {
                                Thread envioThread = new Thread(EnviarLoteAoServidor)
                                {
                                    Name = $"EnvioBatch-{DateTime.Now:HHmmss}",
                                    IsBackground = true
                                };
                                envioThread.Start();
                            }
                        }

                        lock (lockEstado)
                        {
                            estadoSensores[currentSensorId].UltimoSync = DateTime.Now;
                            estadoSensores[currentSensorId].UltimoHeartbeat = DateTime.Now;
                            estadoSensores[currentSensorId].TotalMedicoes++; // ✨ NOVO
                            GuardarEstadoSensor(currentSensorId);
                        }

                        writer.WriteLine("DATA_ACK|SUCESSO");
                        writer.Flush();
                        break;

                    case "LOTE":
                        if (!isConnected || currentSensorId == null || parts.Length < 4)
                        {
                            writer.WriteLine("LOTE_ACK|ERRO|NAO_CONECTADO");
                            writer.Flush();
                            break;
                        }

                        try
                        {
                            int qtd = int.Parse(parts[2]);
                            string medicoesBrutas = string.Join("|", parts.Skip(3));
                            var medicoes = medicoesBrutas.Split('#');

                            int processadas = 0;

                            foreach (var med in medicoes)
                            {
                                if (string.IsNullOrWhiteSpace(med)) continue;

                                var medPartes = med.Split('|');
                                if (medPartes.Length >= 2)
                                {
                                    string tipo = medPartes[0].Trim().ToUpper();
                                    
                                    if (currentTipos.Contains(tipo))
                                    {
                                        GuardarHistorico($"DATA|{currentSensorId}|{medPartes[2]}|{tipo}|{medPartes[1]}");

                                        lock (lockBuffer)
                                        {
                                            _dataBuffer.Add($"DATA|{currentSensorId}|{medPartes[2]}|{tipo}|{medPartes[1]}");
                                        }
                                        
                                        processadas++;
                                    }
                                }
                            }

                            lock (lockEstado)
                            {
                                estadoSensores[currentSensorId].UltimoSync = DateTime.Now;
                                estadoSensores[currentSensorId].UltimoHeartbeat = DateTime.Now;
                                CarregarEstadosPersistidos();
                                GuardarEstadoSensor(currentSensorId);
                            }

                            Console.WriteLine($"[GATEWAY] 📦 Lote de {processadas} medições recebido de {currentSensorId}");
                            writer.WriteLine("LOTE_ACK|SUCESSO");
                            writer.Flush();

                            lock (lockBuffer)
                            {
                                if (_dataBuffer.Count >= MAX_BATCH_SIZE)
                                {
                                    Thread envioThread = new Thread(EnviarLoteAoServidor)
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
                            Console.WriteLine($"[GATEWAY] ✗ Erro ao processar lote: {ex.Message}");
                            writer.WriteLine("LOTE_ACK|ERRO");
                            writer.Flush();
                        }
                        break;

                    case "HEARTBEAT":
                        if (!isConnected || currentSensorId == null)
                        {
                            writer.WriteLine("ACK_HEARTBEAT|ERRO");
                            writer.Flush();
                            break;
                        }

                        lock (lockEstado)
                        {
                            estadoSensores[currentSensorId].UltimoHeartbeat = DateTime.Now;
                            CarregarEstadosPersistidos();
                            GuardarEstadoSensor(currentSensorId);
                        }

                        writer.WriteLine("ACK_HEARTBEAT|SUCESSO");
                        writer.Flush();
                        break;

                    case "NOTIFY":
                        if (!isConnected || currentSensorId == null)
                        {
                            writer.WriteLine("ACK_NOTIFY|ERRO");
                            writer.Flush();
                            break;
                        }

                        if (parts.Length >= 4)
                        {
                            if (parts[3] == "LOW_BATTERY")
                            {
                                lock (lockEstado)
                                {
                                    estadoSensores[currentSensorId].Bateria = int.Parse(parts[4] ?? "20"); // NOVO
                                    estadoSensores[currentSensorId].Estado = "manutencao";
                                    GuardarEstadoSensor(currentSensorId);
                                }

                                Console.WriteLine($"[GATEWAY] ⚠️  Bateria baixa em {currentSensorId}: {estadoSensores[currentSensorId].Bateria}%");
                            }
                        }

                        writer.WriteLine("ACK_NOTIFY|SUCESSO");
                        writer.Flush();
                        break;

                    case "DISCONNECT":
                        if (isConnected && currentSensorId != null)
                        {
                            Console.WriteLine($"[GATEWAY] 👋 Sensor {currentSensorId} desconectado");
                            
                            lock (lockEstado)
                            {
                                estadoSensores[currentSensorId].Estado = "offline";
                                GuardarEstadoSensor(currentSensorId);
                            }

                            writer.WriteLine("ACK_DISCONNECT|SUCESSO");
                            writer.Flush();
                        }
                        return;

                    case "VIDEO":
                        if (!isConnected || currentSensorId == null)
                        {
                            writer.WriteLine("VIDEO_ACK|ERRO");
                            writer.Flush();
                            break;
                        }

                        Console.WriteLine($"[GATEWAY] 🎥 Pedido de stream de vídeo do sensor {currentSensorId}");
                        writer.WriteLine("VIDEO_ACK|ENCAMINHADO_SERVIDOR");
                        writer.Flush();
                        break;
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[GATEWAY] ✗ Erro na ligação: {ex.Message}");
        }
    }
}

void EnviarLoteAoServidor()
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
                Console.WriteLine($"[GATEWAY] 📤 Lote de {batch.Count} enviado ao servidor");
            }
            server.Close();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[GATEWAY] ⚠️  Servidor offline. Dados no histórico local.");
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
            
            lock (lockBuffer)
            {
                if (_dataBuffer.Count > 0)
                {
                    Console.WriteLine($"[GATEWAY] ⏱ Timeout atingido - Enviando {_dataBuffer.Count} medições...");
                    EnviarLoteAoServidor();
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[GATEWAY] ✗ Erro no timer: {ex.Message}");
        }
    }
}

void MonitorizarHeartbeats()
{
    while (gatewayEmExecucao)
    {
        try
        {
            Thread.Sleep(10000);

            lock (lockEstado)
            {
                foreach (var kvp in estadoSensores)
                {
                    var sensor = kvp.Value;
                    
                    if (sensor.Estado == "ativo")
                    {
                        var tempoSemHeartbeat = (DateTime.Now - sensor.UltimoHeartbeat).TotalSeconds;
                        
                        if (tempoSemHeartbeat > HEARTBEAT_TIMEOUT_SECONDS)
                        {
                            Console.WriteLine($"[GATEWAY] ⚠️  Sensor {sensor.SensorId} sem resposta ({tempoSemHeartbeat:F0}s)");
                            sensor.Estado = "timeout";
                            CarregarEstadosPersistidos();
                            GuardarEstadoSensor(sensor.SensorId);
                        }
                    }
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[GATEWAY] ✗ Erro na monitorização: {ex.Message}");
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
            
            if (string.IsNullOrEmpty(cmd)) continue;

            if (cmd.ToLower() == "send")
            {
                Console.WriteLine("[GATEWAY] 📤 Forçando envio do lote...");
                EnviarLoteAoServidor();
            }
            else if (cmd.ToLower() == "status")
            {
                Console.WriteLine("\n===== ESTADO DOS SENSORES =====");
                lock (lockEstado)
                {
                    foreach (var kvp in estadoSensores)
                    {
                        var sensor = kvp.Value;
                        Console.WriteLine($"\n{sensor.SensorId} - {sensor.Zona}");
                        Console.WriteLine($"  Estado: {sensor.Estado}");
                        Console.WriteLine($"  Bateria: {sensor.Bateria}%");
                        Console.WriteLine($"  Medições: {sensor.TotalMedicoes}");
                        Console.WriteLine($"  Último sync: {sensor.UltimoSync:yyyy-MM-dd HH:mm:ss}");
                        Console.WriteLine($"  Último heartbeat: {sensor.UltimoHeartbeat:yyyy-MM-dd HH:mm:ss}");
                    }
                }
                Console.WriteLine("================================\n");
            }
            else if (cmd.ToLower() == "pedidos")
            {
                MostrarPedidosPendentes();
            }
            else if (cmd.ToLower().StartsWith("aceitar "))
            {
                string sensorId = cmd.Substring(8).Trim();
                AceitarSensor(sensorId);
            }
            else if (cmd.ToLower().StartsWith("rejeitar "))
            {
                string sensorId = cmd.Substring(9).Trim();
                RejeitarSensor(sensorId);
            }
        }
        catch { }
    }
}

void MostrarPedidosPendentes()
{
    lock (lockPedidos)
    {
        if (pedidosPendentes.Count == 0)
        {
            Console.WriteLine("[GATEWAY] ℹ️  Nenhum pedido pendente\n");
            return;
        }

        Console.WriteLine("\n===== PEDIDOS PENDENTES =====");
        foreach (var pedido in pedidosPendentes.Where(p => p.Status == "pendente"))
        {
            Console.WriteLine($"• {pedido.SensorId} - Zona: {pedido.Zona} | Tipos: {pedido.TiposDados}");
        }
        Console.WriteLine("============================\n");
    }
}

void AceitarSensor(string sensorId)
{
    lock (lockPedidos)
    {
        var pedido = pedidosPendentes.FirstOrDefault(p => p.SensorId == sensorId && p.Status == "pendente");
        
        if (pedido == null)
        {
            Console.WriteLine($"[GATEWAY] ⚠️  Nenhum pedido pendente para {sensorId}");
            return;
        }

        // Adicionar à config
        lock (lockConfig)
        {
            var novoSensor = new SensorRegistado
            {
                SensorId = pedido.SensorId,
                Zona = pedido.Zona,
                TiposDados = pedido.TiposDados
            };

            var novosSensores = gatewayConfig.SensoresRegistados.ToList();
            novosSensores.Add(novoSensor);
            gatewayConfig.SensoresRegistados = novosSensores.ToArray();

            GuardarGatewayConfig();

            // Adicionar ao dicionário de estado
            estadoSensores[sensorId] = new SensorEstado
            {
                SensorId = sensorId,
                Zona = pedido.Zona,
                TiposDados = pedido.TiposDados.Split(',').Select(t => t.Trim()).ToList(),
                Estado = "offline",
                UltimoHeartbeat = DateTime.Now,
                UltimoSync = DateTime.MinValue
            };
            CarregarEstadosPersistidos();
            GuardarEstadoSensor(sensorId);

            pedido.Status = "aceite";
            GuardarPedidosPendentes();

            Console.WriteLine($"[GATEWAY] ✅ Sensor {sensorId} aceite e registado!");
        }
    }
}

void RejeitarSensor(string sensorId)
{
    lock (lockPedidos)
    {
        var pedido = pedidosPendentes.FirstOrDefault(p => p.SensorId == sensorId && p.Status == "pendente");
        
        if (pedido == null)
        {
            Console.WriteLine($"[GATEWAY] ⚠️  Nenhum pedido pendente para {sensorId}");
            return;
        }

        pedido.Status = "rejeitado";
        GuardarPedidosPendentes();
        Console.WriteLine($"[GATEWAY] ❌ Sensor {sensorId} rejeitado");
    }
}

void GuardarGatewayConfig()
{
    try
    {
        string json = JsonSerializer.Serialize(gatewayConfig, new JsonSerializerOptions { WriteIndented = true });
        File.WriteAllText(GATEWAY_CONFIG_FILE, json);
        Console.WriteLine($"[GATEWAY] 💾 gateway_config.json atualizado");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[GATEWAY] ✗ Erro ao guardar configuração: {ex.Message}");
    }
}

void GuardarEstadoSensor(string sensorId)
{
    try
    {
        // Construir o dicionário completo de todos os sensores
        var estadosCentralizados = new
        {
            ultima_atualizacao = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss"),
            sensores = estadoSensores.ToDictionary(
                kvp => kvp.Key,
                kvp => new
                {
                    sensor_id = kvp.Value.SensorId,
                    zona = kvp.Value.Zona,
                    tipos_dados = kvp.Value.TiposDados,
                    estado = kvp.Value.Estado,
                    bateria = kvp.Value.Bateria,
                    ultimo_heartbeat = kvp.Value.UltimoHeartbeat.ToString("yyyy-MM-ddTHH:mm:ss"),
                    ultimo_sync = kvp.Value.UltimoSync.ToString("yyyy-MM-ddTHH:mm:ss"),
                    data_conexao = kvp.Value.DataConexao.ToString("yyyy-MM-ddTHH:mm:ss"),
                    total_medicoes = kvp.Value.TotalMedicoes,
                    erros_comunicacao = kvp.Value.ErrosComunicacao
                }
            )
        };

        string json = JsonSerializer.Serialize(estadosCentralizados, new JsonSerializerOptions { WriteIndented = true });
        string caminhoArquivo = Path.Combine(ESTADO_DIR, "sensores_estado.json");
        File.WriteAllText(caminhoArquivo, json);
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[GATEWAY] ✗ Erro ao guardar estado: {ex.Message}");
    }
}

// Carregar estado ao iniciar

void CarregarEstadosPersistidos()
{
    try
    {
        string caminhoArquivo = Path.Combine(ESTADO_DIR, "sensores_estado.json");

        if (!File.Exists(caminhoArquivo))
        {
            Console.WriteLine($"[GATEWAY] ℹ️  Nenhum estado persistido encontrado");
            return;
        }

        string json = File.ReadAllText(caminhoArquivo);
        var estadoCentral = JsonSerializer.Deserialize<JsonElement>(json);

        if (estadoCentral.TryGetProperty("sensores", out var sensoresJson))
        {
            foreach (var sensorProp in sensoresJson.EnumerateObject())
            {
                string sensorId = sensorProp.Name;

                if (!estadoSensores.ContainsKey(sensorId))
                    continue;

                var sensor = estadoSensores[sensorId];
                var sensorData = sensorProp.Value;

                // Carregar cada campo
                if (sensorData.TryGetProperty("estado", out var estadoProp))
                    sensor.Estado = estadoProp.GetString() ?? "offline";

                if (sensorData.TryGetProperty("bateria", out var bateriaProp))
                    sensor.Bateria = bateriaProp.GetInt32();

                if (sensorData.TryGetProperty("ultimo_heartbeat", out var hbProp))
                    if (DateTime.TryParse(hbProp.GetString(), out var hbTime))
                        sensor.UltimoHeartbeat = hbTime;

                if (sensorData.TryGetProperty("ultimo_sync", out var syncProp))
                    if (DateTime.TryParse(syncProp.GetString(), out var syncTime))
                        sensor.UltimoSync = syncTime;

                if (sensorData.TryGetProperty("data_conexao", out var connProp))
                    if (DateTime.TryParse(connProp.GetString(), out var connTime))
                        sensor.DataConexao = connTime;

                if (sensorData.TryGetProperty("total_medicoes", out var medProp))
                    sensor.TotalMedicoes = medProp.GetInt32();

                if (sensorData.TryGetProperty("erros_comunicacao", out var erroProp))
                    sensor.ErrosComunicacao = erroProp.GetInt32();

                Console.WriteLine($"[GATEWAY] ✓ Estado do sensor {sensorId} carregado (Bateria: {sensor.Bateria}%)");
            }
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[GATEWAY] ⚠️  Erro ao carregar estados: {ex.Message}");
    }
}

// Atualizar inicialização dos sensores

foreach (var sensor in gatewayConfig.SensoresRegistados)
{
    estadoSensores[sensor.SensorId] = new SensorEstado
    {
        SensorId = sensor.SensorId,
        Zona = sensor.Zona,
        TiposDados = sensor.TiposDados.Split(',').Select(t => t.Trim()).ToList(),
        Estado = "offline",
        Bateria = 100,
        UltimoHeartbeat = DateTime.Now,
        UltimoSync = DateTime.MinValue,
        DataConexao = DateTime.MinValue,
        TotalMedicoes = 0,
        ErrosComunicacao = 0
    };
}


void GuardarHistorico(string dados)
{
    lock (lockHistorico)
    {
        try
        {
            File.AppendAllText(HISTORY_FILE, $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {dados}{Environment.NewLine}");
        }
        catch { }
    }
}

void GuardarPedidosPendentes()
{
    try
    {
        string json = JsonSerializer.Serialize(pedidosPendentes, new JsonSerializerOptions { WriteIndented = true });
        File.WriteAllText(PEDIDOS_PENDENTES_FILE, json);
    }
    catch { }
}

void CarregarPedidosPendentes()
{
    try
    {
        if (File.Exists(PEDIDOS_PENDENTES_FILE))
        {
            string json = File.ReadAllText(PEDIDOS_PENDENTES_FILE);
            var pedidos = JsonSerializer.Deserialize<List<PedidoSensor>>(json);
            if (pedidos != null)
            {
                pedidosPendentes = pedidos;
                Console.WriteLine($"[GATEWAY] ✓ {pedidosPendentes.Count(p => p.Status == "pendente")} pedidos carregados");
            }
        }
    }
    catch { }
}

GatewayConfiguracao? CarregarGatewayConfig()
{
    try
    {
        if (File.Exists(GATEWAY_CONFIG_FILE))
        {
            string json = File.ReadAllText(GATEWAY_CONFIG_FILE);
            return JsonSerializer.Deserialize<GatewayConfiguracao>(json);
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[GATEWAY] ✗ Erro ao ler configuração: {ex.Message}");
    }

    return null;
}

class GatewayConfiguracao
{
    [JsonPropertyName("zonas_validas")]
    public string[] ZonasValidas { get; set; } = Array.Empty<string>();

    [JsonPropertyName("tipos_dados_suportados")]
    public string[] TiposDadosSuportados { get; set; } = Array.Empty<string>();

    [JsonPropertyName("sensores_registados")]
    public SensorRegistado[] SensoresRegistados { get; set; } = Array.Empty<SensorRegistado>();

    [JsonPropertyName("parametros_gateway")]
    public ParametrosGateway ParametrosGateway { get; set; } = new();
}

class SensorRegistado
{
    [JsonPropertyName("sensor_id")]
    public string SensorId { get; set; } = "";

    [JsonPropertyName("zona")]
    public string Zona { get; set; } = "";

    [JsonPropertyName("tipos_dados")]
    public string TiposDados { get; set; } = "";
}

class ParametrosGateway
{
    [JsonPropertyName("max_batch_size")]
    public int MaxBatchSize { get; set; } = 5;

    [JsonPropertyName("batch_timeout_ms")]
    public int BatchTimeoutMs { get; set; } = 300000;

    [JsonPropertyName("heartbeat_timeout_seconds")]
    public int HeartbeatTimeoutSeconds { get; set; } = 60;
}

class SensorEstado
{
    public string SensorId { get; set; } = "";
    public string Zona { get; set; } = "";
    public List<string> TiposDados { get; set; } = new();
    public string Estado { get; set; } = "offline";
    public int Bateria { get; set; } = 100;
    public DateTime UltimoHeartbeat { get; set; }
    public DateTime UltimoSync { get; set; }
    public DateTime DataConexao { get; set; }
    public int TotalMedicoes { get; set; } = 0;
    public int ErrosComunicacao { get; set; } = 0;
}


class PedidoSensor
{
    [JsonPropertyName("sensor_id")]
    public string SensorId { get; set; } = "";

    [JsonPropertyName("zona")]
    public string Zona { get; set; } = "";

    [JsonPropertyName("tipos_dados")]
    public string TiposDados { get; set; } = "";

    [JsonPropertyName("data_pedido")]
    public DateTime DataPedido { get; set; }

    [JsonPropertyName("status")]
    public string Status { get; set; } = "pendente"; // pendente, aceite, rejeitado
}