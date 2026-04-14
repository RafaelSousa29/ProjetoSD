using System.Diagnostics;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;

const string CONFIG_DIR_SENSORS = "../sensor_configs";
const string GATEWAY_IP = "127.0.0.1";
const int GATEWAY_PORT = 5000;
const string DADOS_DIR = "../dados";
const string CONFIG_DIR = "../dados/config";
const int INTERVALO_LEITURA_MS = 5000;
const int HEARTBEAT_NORMAL_MS = 10000;
const int HEARTBEAT_MANUTENCAO_MS = 180000;
const int NIVEL_BATERIA_INICIAL = 100;
const int LIMITE_BATERIA_CRITICA = 20;
const int CONSUMO_LOTE = 5;
const int CONSUMO_HEARTBEAT = 1;
const int MAX_LOTE_SIZE = 5;
const int TIMEOUT_LOTE_MS = 30000;

// ✨ NOVO: Menu para escolher sensor
string configName = args.Length > 0 ? args[0] : MostrarMenuSensores();

if (string.IsNullOrEmpty(configName))
{
    Console.WriteLine("[SENSOR] Nenhum sensor selecionado. Encerrando.");
    return;
}

// Carregar configuração do sensor específico
var sensorConfig = CarregarOuCriarConfiguracao(configName);

string sensorId = sensorConfig.SensorId;
string zona = sensorConfig.Zona;
string tiposDados = sensorConfig.TiposDados;

string MEDICOES_FILE = Path.Combine(DADOS_DIR, $"medicoes_{sensorId}.txt");
string CONFIG_FILE = Path.Combine(CONFIG_DIR, $"config_{sensorId}.txt");
string SENSOR_CONFIG_FILE = Path.Combine(CONFIG_DIR_SENSORS, $"{configName}_config.json");

TcpClient? client = null;
StreamReader? reader = null;
StreamWriter? writer = null;

bool ligado = false;
bool modoManutencao = false;
bool heartbeatAtivo = false;
bool sensorEmExecucao = true;
bool leituraAutomaticaAtiva = false;
int heartbeatCount = 0;
int nivelBateria = NIVEL_BATERIA_INICIAL;
long ultimaLeitura = 0;

List<string> bufferMedicoes = new();
DateTime ultimoEnvioLote = DateTime.Now;

object lockSocket = new object();
object lockFicheiro = new object();
object lockBuffer = new object();

try
{
    if (!Directory.Exists(CONFIG_DIR))
    {
        Directory.CreateDirectory(CONFIG_DIR);
    }

    try
    {
        File.WriteAllText(CONFIG_FILE, tiposDados);
        Console.WriteLine($"[SENSOR {sensorId}] ✓ Configuração guardada: {tiposDados}");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[SENSOR {sensorId}] ⚠ Erro ao guardar config: {ex.Message}");
    }

    Console.WriteLine($"[SENSOR {sensorId}] A conectar ao gateway...");
    
    client = new TcpClient();
    client.Connect(GATEWAY_IP, GATEWAY_PORT);

    NetworkStream stream = client.GetStream();
    reader = new StreamReader(stream, Encoding.UTF8);
    writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true };

    string connectMsg = $"CONNECT|{sensorId}|{tiposDados}|{zona}";
    writer.WriteLine(connectMsg);
    writer.Flush();

    string? response = reader.ReadLine();
    Console.WriteLine($"[SENSOR {sensorId}] Resposta: {response}");

    if (response == "CONN_ACK|ACEITE")
    {
        ligado = true;
        modoManutencao = false;
        Console.WriteLine($"[SENSOR {sensorId}] ✓ Ligação aceite em modo normal.");
        Console.WriteLine($"[SENSOR {sensorId}] ID: {sensorId} | Zona: {zona}");
        Console.WriteLine($"[SENSOR {sensorId}] Ficheiro: {MEDICOES_FILE}");
        Console.WriteLine($"[SENSOR {sensorId}] Tipos activos: {tiposDados}");
        Console.WriteLine($"[SENSOR {sensorId}] Sistema de lotes: MAX={MAX_LOTE_SIZE} | TIMEOUT={TIMEOUT_LOTE_MS}ms");
        Console.WriteLine($"[SENSOR {sensorId}] Bateria: {nivelBateria}%\n");

        heartbeatAtivo = true;
        Thread heartbeatThread = new Thread(HeartbeatLoop)
        {
            Name = $"Heartbeat-{sensorId}",
            IsBackground = true
        };
        heartbeatThread.Start();

        Thread timeoutLoteThread = new Thread(VerificarTimeoutLote)
        {
            Name = $"TimeoutLote-{sensorId}",
            IsBackground = true
        };
        timeoutLoteThread.Start();
    }
    else if (response == "CONN_ACK|MANUTENCAO")
    {
        ligado = true;
        modoManutencao = true;
        Console.WriteLine($"[SENSOR {sensorId}] ✓ Ligação aceite em modo manutenção.");
        Console.WriteLine($"[SENSOR {sensorId}] ID: {sensorId} | Zona: {zona}");
        Console.WriteLine($"[SENSOR {sensorId}] Bateria: {nivelBateria}%\n");
    }
    else
    {
        Console.WriteLine($"[SENSOR {sensorId}] ✗ Ligação recusada. Encerrando.");
        reader?.Dispose();
        writer?.Dispose();
        client?.Dispose();
        return;
    }
}
catch (Exception ex)
{
    Console.WriteLine($"[SENSOR {sensorId}] ✗ Erro ao ligar ao gateway: {ex.Message}");
    return;
}

while (sensorEmExecucao && ligado)
{
    Console.WriteLine();
    Console.WriteLine($"===== SENSOR {sensorId} =====");
    Console.WriteLine($"Bateria: {nivelBateria}%");
    Console.WriteLine($"Modo: {(modoManutencao ? "MANUTENÇÃO" : "NORMAL")}");
    Console.WriteLine($"Leitura Automática: {(leituraAutomaticaAtiva ? "✓ ATIVA" : "✗ Inativa")}");
    
    lock (lockBuffer)
    {
        Console.WriteLine($"Buffer: {bufferMedicoes.Count}/{MAX_LOTE_SIZE} medições");
    }

    if (!modoManutencao)
    {
        Console.WriteLine("\n1 - Enviar medição manual");
        Console.WriteLine("2 - Iniciar/Parar leitura automática do ficheiro");
        Console.WriteLine("3 - Forçar envio do lote atual");
        Console.WriteLine("4 - Ver estado das threads");
        Console.WriteLine("5 - Desligar");
        Console.Write("Opção: ");
    }
    else
    {
        Console.WriteLine("\n1 - Enviar medição manual");
        Console.WriteLine("2 - Enviar heartbeat manual");
        Console.WriteLine("3 - Ver estado das threads");
        Console.WriteLine("4 - Desligar");
        Console.Write("Opção: ");
    }

    string? opcao = Console.ReadLine();

    try
    {
        switch (opcao)
        {
            case "1":
                EnviarMedicaoManual();
                break;

            case "2":
                if (!modoManutencao)
                {
                    if (leituraAutomaticaAtiva)
                    {
                        leituraAutomaticaAtiva = false;
                        Console.WriteLine($"[SENSOR {sensorId}] ⏹ Parando leitura automática...");
                        Thread.Sleep(1000);
                    }
                    else
                    {
                        leituraAutomaticaAtiva = true;
                        Thread leituraThread = new Thread(LerMedicoesAutomatico)
                        {
                            Name = $"LeituraMedicoes-{sensorId}",
                            IsBackground = true
                        };
                        leituraThread.Start();
                        Console.WriteLine($"[SENSOR {sensorId}] ▶ Leitura automática iniciada...");
                    }
                }
                else
                {
                    EnviarHeartbeatManual();
                }
                break;

            case "3":
                if (!modoManutencao)
                {
                    Console.WriteLine($"[SENSOR {sensorId}] 📤 Forçando envio do lote...");
                    EnviarLote();
                }
                else
                {
                    MostrarEstadoThreads();
                }
                break;

            case "4":
                if (!modoManutencao)
                {
                    MostrarEstadoThreads();
                }
                else
                {
                    Console.WriteLine($"[SENSOR {sensorId}] A desligar...");
                    sensorEmExecucao = false;
                    leituraAutomaticaAtiva = false;
                }
                break;

            case "5":
                if (!modoManutencao)
                {
                    Console.WriteLine($"[SENSOR {sensorId}] A desligar...");
                    sensorEmExecucao = false;
                    leituraAutomaticaAtiva = false;
                }
                break;

            default:
                Console.WriteLine("Opção inválida.");
                break;
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[SENSOR {sensorId}] ✗ Erro: {ex.Message}");
    }

    if (nivelBateria <= 0)
    {
        Console.WriteLine($"\n[SENSOR {sensorId}] ✗ Bateria completamente descarregada. Desligando...");
        sensorEmExecucao = false;
    }
}

DesligarSensor();
Console.WriteLine($"\n[SENSOR {sensorId}] Programa encerrado.");
return;

// ✨ NOVO: Menu para escolher sensor
string MostrarMenuSensores()
{
    if (!Directory.Exists(CONFIG_DIR_SENSORS))
    {
        Directory.CreateDirectory(CONFIG_DIR_SENSORS);
    }

    // Listar sensores existentes
    var ficheirosConfig = Directory.GetFiles(CONFIG_DIR_SENSORS, "*_config.json");
    var sensoresExistentes = new List<string>();

    Console.WriteLine("\n===== GERENCIADOR DE SENSORES =====\n");

    if (ficheirosConfig.Length > 0)
    {
        Console.WriteLine("Sensores existentes:\n");
        
        for (int i = 0; i < ficheirosConfig.Length; i++)
        {
            string nome = Path.GetFileNameWithoutExtension(ficheirosConfig[i]).Replace("_config", "");
            sensoresExistentes.Add(nome);
            
            try
            {
                string json = File.ReadAllText(ficheirosConfig[i]);
                var config = JsonSerializer.Deserialize<SensorConfiguracao>(json);
                Console.WriteLine($"{i + 1}. {nome} - Zona: {config?.Zona} | Tipos: {config?.TiposDados}");
            }
            catch
            {
                Console.WriteLine($"{i + 1}. {nome}");
            }
        }

        Console.WriteLine($"\n{ficheirosConfig.Length + 1}. [+] Criar novo sensor");
        Console.WriteLine($"{ficheirosConfig.Length + 2}. [E] Editar sensor existente");
        Console.WriteLine($"{ficheirosConfig.Length + 3}. [X] Sair");
    }
    else
    {
        Console.WriteLine("Nenhum sensor existente.\n");
        Console.WriteLine("1. [+] Criar novo sensor");
        Console.WriteLine("2. [X] Sair");
    }

    Console.Write("\nEscolha: ");
    string? escolha = Console.ReadLine()?.Trim();

    if (string.IsNullOrEmpty(escolha))
        return "";

    // Opção: Sair
    if (escolha.ToUpper() == "X")
    {
        return "";
    }

    // Opção: Editar sensor
    if (escolha.ToUpper() == "E")
    {
        if (sensoresExistentes.Count == 0)
        {
            Console.WriteLine("Nenhum sensor para editar.");
            return "";
        }

        Console.Write("\nQual sensor deseja editar (ex: S101)? ");
        string? sensorEditar = Console.ReadLine()?.Trim();
        
        if (string.IsNullOrEmpty(sensorEditar))
            return "";

        return sensorEditar;
    }

    // Opção: Selecionar existente ou criar novo
    if (int.TryParse(escolha, out int opcao))
    {
        if (opcao > 0 && opcao <= sensoresExistentes.Count)
        {
            return sensoresExistentes[opcao - 1];
        }
        else if (opcao == sensoresExistentes.Count + 1)
        {
            return CriarNovoSensor();
        }
    }

    return "";
}

// ✨ NOVO: Criar novo sensor
string CriarNovoSensor()
{
    Console.WriteLine("\n===== CRIAR NOVO SENSOR =====\n");

    Console.Write("ID do sensor (ex: S101): ");
    string? sensorId = Console.ReadLine()?.Trim();
    
    if (string.IsNullOrEmpty(sensorId))
        return "";

    Console.Write("Zona (ex: ZONA_PARQUE): ");
    string? zona = Console.ReadLine()?.Trim() ?? "ZONA_PARQUE";

    Console.Write("Tipos de dados (ex: TEMP,RUIDO): ");
    string? tiposDados = Console.ReadLine()?.Trim() ?? "TEMP";

    var config = new SensorConfiguracao
    {
        SensorId = sensorId,
        Zona = zona,
        TiposDados = tiposDados
    };

    // Guardar configuração
    if (!Directory.Exists(CONFIG_DIR_SENSORS))
    {
        Directory.CreateDirectory(CONFIG_DIR_SENSORS);
    }

    string configPath = Path.Combine(CONFIG_DIR_SENSORS, $"{sensorId}_config.json");
    
    try
    {
        string json = JsonSerializer.Serialize(config, new JsonSerializerOptions { WriteIndented = true });
        File.WriteAllText(configPath, json);
        Console.WriteLine($"\n✓ Sensor '{sensorId}' criado com sucesso!");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"\n✗ Erro ao criar sensor: {ex.Message}");
        return "";
    }

    return sensorId;
}

SensorConfiguracao CarregarOuCriarConfiguracao(string configName)
{
    if (!Directory.Exists(CONFIG_DIR_SENSORS))
    {
        Directory.CreateDirectory(CONFIG_DIR_SENSORS);
    }

    string configPath = Path.Combine(CONFIG_DIR_SENSORS, $"{configName}_config.json");

    // Tentar carregar do ficheiro
    if (File.Exists(configPath))
    {
        try
        {
            string json = File.ReadAllText(configPath);
            var configCarregada = JsonSerializer.Deserialize<SensorConfiguracao>(json);
            
            if (configCarregada != null && !string.IsNullOrEmpty(configCarregada.SensorId))
            {
                Console.WriteLine($"[SENSOR] ✓ Configuração carregada de '{configPath}'");
                Console.WriteLine($"  ID: {configCarregada.SensorId}");
                Console.WriteLine($"  Zona: {configCarregada.Zona}");
                Console.WriteLine($"  Tipos: {configCarregada.TiposDados}\n");
                return configCarregada;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[SENSOR] ⚠ Erro ao ler ficheiro: {ex.Message}");
        }
    }

    // Se não encontrou, pedir manualmente
    Console.WriteLine($"[SENSOR] Nenhuma configuração encontrada para '{configName}'.\n");
    Console.WriteLine("Insira os dados do sensor:\n");

    Console.Write("ID do sensor (ex: S101): ");
    string sensorId = Console.ReadLine()?.Trim() ?? "S101";

    Console.Write("Zona (ex: ZONA_PARQUE): ");
    string zona = Console.ReadLine()?.Trim() ?? "ZONA_PARQUE";

    Console.Write("Tipos de dados (ex: TEMP,RUIDO): ");
    string tiposDados = Console.ReadLine()?.Trim() ?? "TEMP";

    var configNova = new SensorConfiguracao
    {
        SensorId = sensorId,
        Zona = zona,
        TiposDados = tiposDados
    };

    // Guardar automaticamente
    try
    {
        string json = JsonSerializer.Serialize(configNova, new JsonSerializerOptions { WriteIndented = true });
        File.WriteAllText(configPath, json);
        Console.WriteLine($"\n[SENSOR] ✓ Configuração guardada em '{configPath}'");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"\n[SENSOR] ⚠ Erro ao guardar ficheiro: {ex.Message}");
    }

    Console.WriteLine();
    return configNova;
}

void EnviarMedicaoManual()
{
    if (!ligado || writer == null || reader == null)
    {
        Console.WriteLine($"[SENSOR {sensorId}] ✗ Sensor não ligado ao gateway.");
        return;
    }

    Console.Write("\nTipo de dado: ");
    string tipo = Console.ReadLine()?.Trim() ?? "TEMP";

    Console.Write("Valor: ");
    string valor = Console.ReadLine()?.Trim() ?? "0";

    string timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");
    string medicao = $"{tipo}|{valor}|{timestamp}";

    lock (lockBuffer)
    {
        bufferMedicoes.Add(medicao);
        Console.WriteLine($"[SENSOR {sensorId}] ✓ Medição adicionada ao buffer ({bufferMedicoes.Count}/{MAX_LOTE_SIZE})");

        if (bufferMedicoes.Count >= MAX_LOTE_SIZE)
        {
            Console.WriteLine($"[SENSOR {sensorId}] 📤 Lote cheio! Enviando...");
            EnviarLote();
        }
    }
}

void EnviarHeartbeatManual()
{
    if (!ligado || writer == null || reader == null)
    {
        Console.WriteLine($"[SENSOR {sensorId}] ✗ Sensor não ligado ao gateway.");
        return;
    }

    if (nivelBateria - CONSUMO_HEARTBEAT <= 0)
    {
        Console.WriteLine($"[SENSOR {sensorId}] ✗ Bateria insuficiente para heartbeat.");
        return;
    }

    string timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");

    lock (lockSocket)
    {
        try
        {
            string hbMsg = $"HEARTBEAT|{sensorId}|{timestamp}";
            writer.WriteLine(hbMsg);
            writer.Flush();

            string? response = reader.ReadLine();
            Console.WriteLine($"[SENSOR {sensorId}] Resposta: {response}");

            nivelBateria -= CONSUMO_HEARTBEAT;
            Console.WriteLine($"[SENSOR {sensorId}] Bateria: {nivelBateria}%");

            if (nivelBateria <= LIMITE_BATERIA_CRITICA && !modoManutencao)
            {
                EnviarNotificacaoBateria();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[SENSOR {sensorId}] ✗ Erro ao enviar heartbeat: {ex.Message}");
        }
    }
}

void EnviarNotificacaoBateria()
{
    if (!ligado || writer == null || reader == null)
    {
        return;
    }

    try
    {
        lock (lockSocket)
        {
            string timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");
            string notifyMsg = $"NOTIFY|{sensorId}|{timestamp}|LOW_BATTERY";
            writer.WriteLine(notifyMsg);
            writer.Flush();

            string? response = reader.ReadLine();
            Console.WriteLine($"\n[SENSOR {sensorId}] ⚠ Notificação de bateria fraca enviada. Resposta: {response}");

            modoManutencao = true;
            heartbeatAtivo = false;
            leituraAutomaticaAtiva = false;
            Console.WriteLine($"[SENSOR {sensorId}] ⚠ Entrando em modo de manutenção...");
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[SENSOR {sensorId}] ✗ Erro ao enviar notificação: {ex.Message}");
    }
}

void LerMedicoesAutomatico()
{
    while (leituraAutomaticaAtiva && sensorEmExecucao && ligado)
    {
        try
        {
            if (nivelBateria <= 0)
            {
                Console.WriteLine($"\n[SENSOR {sensorId}] ✗ Bateria descarregada. Parando leitura automática...");
                leituraAutomaticaAtiva = false;
                break;
            }

            if (File.Exists(MEDICOES_FILE))
            {
                lock (lockFicheiro)
                {
                    try
                    {
                        var linhas = File.ReadAllLines(MEDICOES_FILE);
                        
                        if (linhas.Length > ultimaLeitura)
                        {
                            for (long i = ultimaLeitura; i < linhas.Length; i++)
                            {
                                if (!ligado || !sensorEmExecucao || nivelBateria <= 0 || !leituraAutomaticaAtiva)
                                    break;

                                string medicao = linhas[i].Trim();
                                
                                if (string.IsNullOrEmpty(medicao))
                                    continue;

                                var partes = medicao.Split('|');
                                if (partes.Length == 2)
                                {
                                    string tipo = partes[0].Trim();
                                    string valor = partes[1].Trim();
                                    string timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");

                                    lock (lockBuffer)
                                    {
                                        bufferMedicoes.Add($"{tipo}|{valor}|{timestamp}");
                                        Console.WriteLine($"[SENSOR {sensorId}-AUTO] {tipo}={valor} | Buffer: {bufferMedicoes.Count}/{MAX_LOTE_SIZE}");

                                        if (bufferMedicoes.Count >= MAX_LOTE_SIZE)
                                        {
                                            Console.WriteLine($"[SENSOR {sensorId}-AUTO] 📤 Lote cheio! Enviando...");
                                            EnviarLote();
                                        }
                                    }
                                }

                                ultimaLeitura = i + 1;
                            }
                        }
                    }
                    catch (IOException)
                    {
                        // Ficheiro está sendo escrito
                    }
                }
            }
            else
            {
                Console.WriteLine($"\n[SENSOR {sensorId}] ⚠ Ficheiro não encontrado: {MEDICOES_FILE}");
                leituraAutomaticaAtiva = false;
            }

            Thread.Sleep(INTERVALO_LEITURA_MS);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"\n[SENSOR {sensorId}] ✗ Erro no loop automático: {ex.Message}");
            Thread.Sleep(2000);
        }
    }

    leituraAutomaticaAtiva = false;
}

void EnviarLote()
{
    lock (lockBuffer)
    {
        if (bufferMedicoes.Count == 0)
        {
            Console.WriteLine($"[SENSOR {sensorId}] ⚠ Buffer vazio, nada a enviar.");
            return;
        }

        if (!ligado || writer == null || reader == null)
        {
            Console.WriteLine($"[SENSOR {sensorId}] ✗ Sensor não ligado ao gateway.");
            return;
        }

        if (nivelBateria - CONSUMO_LOTE <= 0)
        {
            Console.WriteLine($"[SENSOR {sensorId}] ✗ Bateria insuficiente para enviar lote.");
            return;
        }

        lock (lockSocket)
        {
            try
            {
                string lote = string.Join("#", bufferMedicoes);
                string loteMsg = $"LOTE|{sensorId}|{bufferMedicoes.Count}|{lote}";
                
                writer.WriteLine(loteMsg);
                writer.Flush();

                string? response = reader.ReadLine();
                Console.WriteLine($"[SENSOR {sensorId}] 📥 Resposta: {response}");

                nivelBateria -= CONSUMO_LOTE;
                Console.WriteLine($"[SENSOR {sensorId}] Bateria: {nivelBateria}% (após envio de lote)");

                bufferMedicoes.Clear();
                ultimoEnvioLote = DateTime.Now;

                if (nivelBateria <= LIMITE_BATERIA_CRITICA && !modoManutencao)
                {
                    EnviarNotificacaoBateria();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[SENSOR {sensorId}] ✗ Erro ao enviar lote: {ex.Message}");
            }
        }
    }
}

void VerificarTimeoutLote()
{
    while (sensorEmExecucao && ligado)
    {
        try
        {
            Thread.Sleep(TIMEOUT_LOTE_MS);

            lock (lockBuffer)
            {
                if (bufferMedicoes.Count > 0 && (DateTime.Now - ultimoEnvioLote).TotalMilliseconds > TIMEOUT_LOTE_MS)
                {
                    Console.WriteLine($"\n[SENSOR {sensorId}-TIMEOUT] ⏱ Timeout atingido! Enviando {bufferMedicoes.Count} medições...");
                    EnviarLote();
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[SENSOR {sensorId}-TIMEOUT] ✗ Erro: {ex.Message}");
        }
    }
}

void HeartbeatLoop()
{
    while (heartbeatAtivo && ligado)
    {
        try
        {
            int intervaloHeartbeat = modoManutencao ? HEARTBEAT_MANUTENCAO_MS : HEARTBEAT_NORMAL_MS;
            Thread.Sleep(intervaloHeartbeat);

            if (!heartbeatAtivo || !ligado || writer == null || reader == null)
                continue;

            if (nivelBateria - CONSUMO_HEARTBEAT <= 0)
            {
                Console.WriteLine($"\n[SENSOR {sensorId}] ✗ Bateria insuficiente para heartbeat. Desligando...");
                heartbeatAtivo = false;
                sensorEmExecucao = false;
                break;
            }

            lock (lockSocket)
            {
                try
                {
                    string timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");
                    string hbMsg = $"HEARTBEAT|{sensorId}|{timestamp}";
                    writer.WriteLine(hbMsg);
                    writer.Flush();

                    string? response = reader.ReadLine();
                    heartbeatCount++;

                    nivelBateria -= CONSUMO_HEARTBEAT;

                    if (response == null || !response.StartsWith("ACK_HEARTBEAT|SUCESSO"))
                    {
                        Console.WriteLine($"\n[SENSOR {sensorId}-HB] ✗ Erro: {response} | Bateria: {nivelBateria}%");
                    }
                    else if (heartbeatCount % 10 == 0)
                    {
                        Console.WriteLine($"\n[SENSOR {sensorId}-HB] ✓ {heartbeatCount} enviados. Bateria: {nivelBateria}%");
                    }

                    if (nivelBateria <= LIMITE_BATERIA_CRITICA && !modoManutencao)
                    {
                        EnviarNotificacaoBateria();
                    }
                }
                catch (IOException)
                {
                    Console.WriteLine($"\n[SENSOR {sensorId}] ✗ Conexão perdida com gateway.");
                    heartbeatAtivo = false;
                    sensorEmExecucao = false;
                    break;
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[SENSOR {sensorId}-HB] ✗ Erro: {ex.Message}");
            break;
        }
    }
}

void MostrarEstadoThreads()
{
    Console.WriteLine($"\n===== ESTADO DAS THREADS - {sensorId} =====");
    Console.WriteLine($"Sensor Ligado: {(ligado ? "✓" : "✗")}");
    Console.WriteLine($"Sensor Ativo: {(sensorEmExecucao ? "✓" : "✗")}");
    Console.WriteLine($"Heartbeat Ativo: {(heartbeatAtivo ? "✓" : "✗")}");
    Console.WriteLine($"Leitura Automática: {(leituraAutomaticaAtiva ? "✓" : "✗")}");
    
    lock (lockBuffer)
    {
        Console.WriteLine($"Buffer Medições: {bufferMedicoes.Count}/{MAX_LOTE_SIZE}");
        if (bufferMedicoes.Count > 0)
        {
            foreach (var med in bufferMedicoes)
            {
                Console.WriteLine($"  - {med}");
            }
        }
    }
    
    Console.WriteLine($"Total de Threads: {Process.GetCurrentProcess().Threads.Count}");
    Console.WriteLine("=============================\n");
}

void DesligarSensor()
{
    try
    {
        heartbeatAtivo = false;
        sensorEmExecucao = false;
        leituraAutomaticaAtiva = false;

        lock (lockBuffer)
        {
            if (bufferMedicoes.Count > 0)
            {
                Console.WriteLine($"\n[SENSOR {sensorId}] 📤 Enviando {bufferMedicoes.Count} medições pendentes...");
                EnviarLote();
            }
        }

        Thread.Sleep(300);

        if (ligado && writer != null && reader != null)
        {
            lock (lockSocket)
            {
                try
                {
                    string discMsg = $"DISCONNECT|{sensorId}";
                    Console.WriteLine($"\n[SENSOR {sensorId}] A enviar: {discMsg}");
                    writer.WriteLine(discMsg);
                    writer.Flush();

                    string? response = reader.ReadLine();
                    Console.WriteLine($"[SENSOR {sensorId}] Resposta: {response}");
                }
                catch { }
            }
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[SENSOR {sensorId}] ✗ Erro ao desligar: {ex.Message}");
    }
    finally
    {
        try { reader?.Dispose(); } catch { }
        try { writer?.Dispose(); } catch { }
        try { client?.Dispose(); } catch { }

        reader = null;
        writer = null;
        client = null;
        ligado = false;
    }
}

class SensorConfiguracao
{
    public string SensorId { get; set; } = "";
    public string Zona { get; set; } = "";
    public string TiposDados { get; set; } = "";
}
