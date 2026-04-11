using System.Diagnostics;
using System.Net.Sockets;
using System.Text;
using System.Threading;

const string GATEWAY_IP = "127.0.0.1";
const int GATEWAY_PORT = 5000;
const string DADOS_DIR = "../dados";
const string CONFIG_DIR = "../dados/config";
const int INTERVALO_LEITURA_MS = 5000; // 5 segundos para ler ficheiro
const int HEARTBEAT_NORMAL_MS = 10000; // 10 segundos
const int HEARTBEAT_MANUTENCAO_MS = 180000; // 3 minutos
const int NIVEL_BATERIA_INICIAL = 100;
const int LIMITE_BATERIA_CRITICA = 20;
const int CONSUMO_LOTE = 5; // Consumo por envio de LOTE
const int CONSUMO_HEARTBEAT = 1;
const int MAX_LOTE_SIZE = 5; // Máximo de medições por lote
const int TIMEOUT_LOTE_MS = 30000; // 30 segundos ou enviar o lote mesmo que incompleto

Console.Write("ID do sensor: ");
string sensorId = Console.ReadLine()?.Trim() ?? "S101";

Console.Write("Zona: ");
string zona = Console.ReadLine()?.Trim() ?? "ZONA_PARQUE";

Console.Write("Tipos de dados que este sensor vai usar (ex: TEMP,RUIDO): ");
string tiposDados = Console.ReadLine()?.Trim() ?? "TEMP";

string MEDICOES_FILE = Path.Combine(DADOS_DIR, $"medicoes_{sensorId}.txt");
string CONFIG_FILE = Path.Combine(CONFIG_DIR, $"config_{sensorId}.txt");

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

// Buffer para lote de medições
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
        Console.WriteLine($"[SENSOR] ✓ Configuração guardada: {tiposDados}");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[SENSOR] ⚠ Erro ao guardar config: {ex.Message}");
    }

    Console.WriteLine("[SENSOR] A conectar ao gateway...");
    
    client = new TcpClient();
    client.Connect(GATEWAY_IP, GATEWAY_PORT);

    NetworkStream stream = client.GetStream();
    reader = new StreamReader(stream, Encoding.UTF8);
    writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true };

    string connectMsg = $"CONNECT|{sensorId}|{tiposDados}|{zona}";
    writer.WriteLine(connectMsg);
    writer.Flush();

    string? response = reader.ReadLine();
    Console.WriteLine($"[SENSOR] Resposta: {response}");

    if (response == "CONN_ACK|ACEITE")
    {
        ligado = true;
        modoManutencao = false;
        Console.WriteLine("[SENSOR] ✓ Ligação aceite em modo normal.");
        Console.WriteLine($"[SENSOR] Ficheiro: {MEDICOES_FILE}");
        Console.WriteLine($"[SENSOR] Tipos activos: {tiposDados}");
        Console.WriteLine($"[SENSOR] Sistema de lotes: MAX={MAX_LOTE_SIZE} | TIMEOUT={TIMEOUT_LOTE_MS}ms");
        Console.WriteLine($"[SENSOR] Consumo: Lote={CONSUMO_LOTE}% | Heartbeat={CONSUMO_HEARTBEAT}%");
        Console.WriteLine($"[SENSOR] Bateria: {nivelBateria}%\n");

        heartbeatAtivo = true;
        Thread heartbeatThread = new Thread(HeartbeatLoop)
        {
            Name = $"Heartbeat-{sensorId}",
            IsBackground = true
        };
        heartbeatThread.Start();

        // Thread para verificar timeout do lote
        Thread timeoutLotetThread = new Thread(VerificarTimeoutLote)
        {
            Name = $"TimeoutLote-{sensorId}",
            IsBackground = true
        };
        timeoutLotetThread.Start();
    }
    else if (response == "CONN_ACK|MANUTENCAO")
    {
        ligado = true;
        modoManutencao = true;
        Console.WriteLine("[SENSOR] ✓ Ligação aceite em modo manutenção.");
        Console.WriteLine($"[SENSOR] Ficheiro: {MEDICOES_FILE}");
        Console.WriteLine($"[SENSOR] Tipos activos: {tiposDados}");
        Console.WriteLine($"[SENSOR] Bateria: {nivelBateria}%\n");
    }
    else
    {
        Console.WriteLine("[SENSOR] ✗ Ligação recusada. Encerrando.");
        reader?.Dispose();
        writer?.Dispose();
        client?.Dispose();
        return;
    }
}
catch (Exception ex)
{
    Console.WriteLine($"[SENSOR] ✗ Erro ao ligar ao gateway: {ex.Message}");
    return;
}

// Loop principal - Menu interativo
while (sensorEmExecucao && ligado)
{
    Console.WriteLine();
    Console.WriteLine("===== SENSOR =====");
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
                        Console.WriteLine("[SENSOR] ⏹ Parando leitura automática...");
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
                        Console.WriteLine("[SENSOR] ▶ Leitura automática iniciada...");
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
                    Console.WriteLine("[SENSOR] 📤 Forçando envio do lote...");
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
                    Console.WriteLine("[SENSOR] A desligar...");
                    sensorEmExecucao = false;
                    leituraAutomaticaAtiva = false;
                }
                break;

            case "5":
                if (!modoManutencao)
                {
                    Console.WriteLine("[SENSOR] A desligar...");
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
        Console.WriteLine($"[SENSOR] ✗ Erro: {ex.Message}");
    }

    if (nivelBateria <= 0)
    {
        Console.WriteLine("\n[SENSOR] ✗ Bateria completamente descarregada. Desligando...");
        sensorEmExecucao = false;
    }
}

DesligarSensor();
Console.WriteLine("\n[SENSOR] Programa encerrado.");
return;

void EnviarMedicaoManual()
{
    if (!ligado || writer == null || reader == null)
    {
        Console.WriteLine("[SENSOR] ✗ Sensor não ligado ao gateway.");
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
        Console.WriteLine($"[SENSOR] ✓ Medição adicionada ao buffer ({bufferMedicoes.Count}/{MAX_LOTE_SIZE})");

        if (bufferMedicoes.Count >= MAX_LOTE_SIZE)
        {
            Console.WriteLine("[SENSOR] 📤 Lote cheio! Enviando...");
            EnviarLote();
        }
    }
}

void EnviarHeartbeatManual()
{
    if (!ligado || writer == null || reader == null)
    {
        Console.WriteLine("[SENSOR] ✗ Sensor não ligado ao gateway.");
        return;
    }

    if (nivelBateria - CONSUMO_HEARTBEAT <= 0)
    {
        Console.WriteLine("[SENSOR] ✗ Bateria insuficiente para heartbeat.");
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
            Console.WriteLine($"[SENSOR] Resposta: {response}");

            nivelBateria -= CONSUMO_HEARTBEAT;
            Console.WriteLine($"[SENSOR] Bateria: {nivelBateria}%");

            if (nivelBateria <= LIMITE_BATERIA_CRITICA && !modoManutencao)
            {
                EnviarNotificacaoBateria();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[SENSOR] ✗ Erro ao enviar heartbeat: {ex.Message}");
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
            Console.WriteLine($"\n[SENSOR] ⚠ Notificação de bateria fraca enviada. Resposta: {response}");

            modoManutencao = true;
            heartbeatAtivo = false;
            leituraAutomaticaAtiva = false;
            Console.WriteLine("[SENSOR] ⚠ Entrando em modo de manutenção...");
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[SENSOR] ✗ Erro ao enviar notificação: {ex.Message}");
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
                Console.WriteLine("\n[SENSOR] ✗ Bateria descarregada. Parando leitura automática...");
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
                                        Console.WriteLine($"[SENSOR-AUTO] {tipo}={valor} | Buffer: {bufferMedicoes.Count}/{MAX_LOTE_SIZE}");

                                        if (bufferMedicoes.Count >= MAX_LOTE_SIZE)
                                        {
                                            Console.WriteLine("[SENSOR-AUTO] 📤 Lote cheio! Enviando...");
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
                Console.WriteLine($"\n[SENSOR] ⚠ Ficheiro não encontrado: {MEDICOES_FILE}");
                leituraAutomaticaAtiva = false;
            }

            Thread.Sleep(INTERVALO_LEITURA_MS);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"\n[SENSOR] ✗ Erro no loop automático: {ex.Message}");
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
            Console.WriteLine("[SENSOR] ⚠ Buffer vazio, nada a enviar.");
            return;
        }

        if (!ligado || writer == null || reader == null)
        {
            Console.WriteLine("[SENSOR] ✗ Sensor não ligado ao gateway.");
            return;
        }

        if (nivelBateria - CONSUMO_LOTE <= 0)
        {
            Console.WriteLine("[SENSOR] ✗ Bateria insuficiente para enviar lote.");
            return;
        }

        lock (lockSocket)
        {
            try
            {
                // Formatar lote: LOTE|sensor_id|qtd|med1#med2#med3
                string lote = string.Join("#", bufferMedicoes);
                string loteMsg = $"LOTE|{sensorId}|{bufferMedicoes.Count}|{lote}";
                
                writer.WriteLine(loteMsg);
                writer.Flush();

                string? response = reader.ReadLine();
                Console.WriteLine($"[SENSOR] 📥 Resposta: {response}");

                nivelBateria -= CONSUMO_LOTE;
                Console.WriteLine($"[SENSOR] Bateria: {nivelBateria}% (após envio de lote)");

                bufferMedicoes.Clear();
                ultimoEnvioLote = DateTime.Now;

                if (nivelBateria <= LIMITE_BATERIA_CRITICA && !modoManutencao)
                {
                    EnviarNotificacaoBateria();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[SENSOR] ✗ Erro ao enviar lote: {ex.Message}");
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
                    Console.WriteLine($"\n[SENSOR-TIMEOUT] ⏱ Timeout atingido! Enviando {bufferMedicoes.Count} medições...");
                    EnviarLote();
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[SENSOR-TIMEOUT] ✗ Erro: {ex.Message}");
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
                Console.WriteLine("\n[SENSOR] ✗ Bateria insuficiente para heartbeat. Desligando...");
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
                        Console.WriteLine($"\n[SENSOR-HB] ✗ Erro: {response} | Bateria: {nivelBateria}%");
                    }
                    else if (heartbeatCount % 10 == 0)
                    {
                        Console.WriteLine($"\n[SENSOR-HB] ✓ {heartbeatCount} enviados. Bateria: {nivelBateria}%");
                    }

                    if (nivelBateria <= LIMITE_BATERIA_CRITICA && !modoManutencao)
                    {
                        EnviarNotificacaoBateria();
                    }
                }
                catch (IOException)
                {
                    Console.WriteLine("\n[SENSOR] ✗ Conexão perdida com gateway.");
                    heartbeatAtivo = false;
                    sensorEmExecucao = false;
                    break;
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[SENSOR-HB] ✗ Erro: {ex.Message}");
            break;
        }
    }
}

void MostrarEstadoThreads()
{
    Console.WriteLine("\n===== ESTADO DAS THREADS =====");
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

        // Enviar lote pendente antes de desligar
        lock (lockBuffer)
        {
            if (bufferMedicoes.Count > 0)
            {
                Console.WriteLine($"\n[SENSOR] 📤 Enviando {bufferMedicoes.Count} medições pendentes...");
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
                    Console.WriteLine($"\n[SENSOR] A enviar: {discMsg}");
                    writer.WriteLine(discMsg);
                    writer.Flush();

                    string? response = reader.ReadLine();
                    Console.WriteLine($"[SENSOR] Resposta: {response}");
                }
                catch { }
            }
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[SENSOR] ✗ Erro ao desligar: {ex.Message}");
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