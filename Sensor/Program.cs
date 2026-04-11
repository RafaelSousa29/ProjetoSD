using System.Net.Sockets;
using System.Text;
using System.Threading;

const string GATEWAY_IP = "127.0.0.1";
const int GATEWAY_PORT = 5000;
const string MEDICOES_FILE = "medicoes.txt";
const int INTERVALO_LEITURA_MS = 5000; // 5 segundos
const int HEARTBEAT_NORMAL_MS = 10000; // 10 segundos
const int HEARTBEAT_MANUTENCAO_MS = 180000; // 3 minutos
const int NIVEL_BATERIA_INICIAL = 100;
const int LIMITE_BATERIA_CRITICA = 20;
const int CONSUMO_DATA = 2; // Consumo por envio de DATA
const int CONSUMO_HEARTBEAT = 1; // Consumo por heartbeat

Console.Write("ID do sensor: ");
string sensorId = Console.ReadLine()?.Trim() ?? "S101";

Console.Write("Zona: ");
string zona = Console.ReadLine()?.Trim() ?? "ZONA_PARQUE";

Console.Write("Tipos de dados (ex: TEMP,RUIDO,HUM): ");
string tiposDados = Console.ReadLine()?.Trim() ?? "TEMP,RUIDO,HUM";

TcpClient? client = null;
StreamReader? reader = null;
StreamWriter? writer = null;

bool ligado = false;
bool modoManutencao = false;
bool heartbeatAtivo = false;
bool sensorEmExecucao = true;
int heartbeatCount = 0;
int nivelBateria = NIVEL_BATERIA_INICIAL;
long ultimaLeitura = 0;

object lockSocket = new object();
object lockFicheiro = new object();

try
{
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
        Console.WriteLine($"[SENSOR] Bateria: {nivelBateria}%");

        heartbeatAtivo = true;
        Thread heartbeatThread = new Thread(HeartbeatLoop)
        {
            Name = $"Heartbeat-{sensorId}",
            IsBackground = false
        };
        heartbeatThread.Start();
    }
    else if (response == "CONN_ACK|MANUTENCAO")
    {
        ligado = true;
        modoManutencao = true;
        Console.WriteLine("[SENSOR] ✓ Ligação aceite em modo manutenção.");
        Console.WriteLine($"[SENSOR] Bateria: {nivelBateria}%");
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

Console.WriteLine("[SENSOR] Iniciando leitura automática do ficheiro...\n");

// Thread para leitura automática de medições
Thread leituraMedicoesThread = new Thread(LerMedicoesAutomatico)
{
    Name = $"LeituraMedicoes-{sensorId}",
    IsBackground = false
};
leituraMedicoesThread.Start();

// Esperar que o sensor finalize naturalmente
leituraMedicoesThread.Join();

// Garantir desligamento
DesligarSensor();
Console.WriteLine("[SENSOR] Programa encerrado.");
return;

void EnviarMedicao(string tipo, string valor)
{
    if (!ligado || writer == null || reader == null)
    {
        Console.WriteLine("[SENSOR] ✗ Sensor não ligado ao gateway.");
        return;
    }

    if (nivelBateria - CONSUMO_DATA <= 0)
    {
        Console.WriteLine("[SENSOR] ✗ Bateria insuficiente.");
        return;
    }

    string timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");

    lock (lockSocket)
    {
        try
        {
            string dataMsg = $"DATA|{sensorId}|{timestamp}|{tipo}|{valor}";
            writer.WriteLine(dataMsg);
            writer.Flush();

            string? response = reader.ReadLine();
            Console.WriteLine($"[SENSOR] {tipo}={valor} | Resposta: {response}");

            // Consumir bateria
            nivelBateria -= CONSUMO_DATA;
            Console.WriteLine($"[SENSOR] Bateria: {nivelBateria}%");

            // Verificar bateria crítica
            if (nivelBateria <= LIMITE_BATERIA_CRITICA && !modoManutencao)
            {
                EnviarNotificacaoBateria();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[SENSOR] ✗ Erro ao enviar medição: {ex.Message}");
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
            Console.WriteLine($"[SENSOR] ⚠ Notificação de bateria fraca enviada. Resposta: {response}");

            // Entrar em modo de manutenção
            modoManutencao = true;
            heartbeatAtivo = false;
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
    while (sensorEmExecucao && ligado)
    {
        try
        {
            if (nivelBateria <= 0)
            {
                Console.WriteLine("[SENSOR] ✗ Bateria descarregada. Desligando automaticamente...");
                sensorEmExecucao = false;
                break;
            }

            // Tentar ler novas linhas do ficheiro
            if (File.Exists(MEDICOES_FILE))
            {
                lock (lockFicheiro)
                {
                    try
                    {
                        var linhas = File.ReadAllLines(MEDICOES_FILE);
                        
                        // Processar apenas linhas novas desde a última leitura
                        if (linhas.Length > ultimaLeitura)
                        {
                            for (long i = ultimaLeitura; i < linhas.Length; i++)
                            {
                                if (!ligado || !sensorEmExecucao || nivelBateria <= 0)
                                    break;

                                string medicao = linhas[i].Trim();
                                
                                if (string.IsNullOrEmpty(medicao))
                                    continue;

                                // Esperado formato: "TIPO|VALOR"
                                var partes = medicao.Split('|');
                                if (partes.Length == 2)
                                {
                                    string tipo = partes[0].Trim();
                                    string valor = partes[1].Trim();

                                    if (nivelBateria - CONSUMO_DATA > 0)
                                    {
                                        EnviarMedicao(tipo, valor);
                                    }
                                    else
                                    {
                                        Console.WriteLine("[SENSOR] ✗ Bateria insuficiente para enviar medição.");
                                        break;
                                    }
                                }

                                ultimaLeitura = i + 1;
                            }
                        }
                    }
                    catch (IOException)
                    {
                        // Ficheiro está sendo escrito, tenta novamente depois
                    }
                }
            }

            // Aguardar antes de verificar novamente
            Thread.Sleep(INTERVALO_LEITURA_MS);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[SENSOR] ✗ Erro no loop principal: {ex.Message}");
            Thread.Sleep(2000);
        }
    }
}

void HeartbeatLoop()
{
    while (heartbeatAtivo && ligado)
    {
        try
        {
            // Determinar intervalo conforme o modo
            int intervaloHeartbeat = modoManutencao ? HEARTBEAT_MANUTENCAO_MS : HEARTBEAT_NORMAL_MS;
            Thread.Sleep(intervaloHeartbeat);

            if (!heartbeatAtivo || !ligado || writer == null || reader == null)
                continue;

            // Verificar se tem bateria
            if (nivelBateria - CONSUMO_HEARTBEAT <= 0)
            {
                Console.WriteLine("[SENSOR] ✗ Bateria insuficiente para heartbeat.");
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

                    // Consumir bateria
                    nivelBateria -= CONSUMO_HEARTBEAT;

                    if (response == null || !response.StartsWith("ACK_HEARTBEAT|SUCESSO"))
                    {
                        Console.WriteLine($"[SENSOR] ✗ Erro no heartbeat: {response} | Bateria: {nivelBateria}%");
                    }
                    else if (heartbeatCount % 10 == 0)
                    {
                        Console.WriteLine($"[SENSOR] ✓ {heartbeatCount} heartbeats enviados. Bateria: {nivelBateria}%");
                    }

                    // Verificar bateria crítica
                    if (nivelBateria <= LIMITE_BATERIA_CRITICA && !modoManutencao)
                    {
                        EnviarNotificacaoBateria();
                    }
                }
                catch (IOException)
                {
                    Console.WriteLine("[SENSOR] ✗ Conexão perdida com gateway.");
                    heartbeatAtivo = false;
                    sensorEmExecucao = false;
                    break;
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[SENSOR] ✗ Erro no heartbeat: {ex.Message}");
            break;
        }
    }
}

void DesligarSensor()
{
    try
    {
        heartbeatAtivo = false;
        sensorEmExecucao = false;

        // Pequena pausa para garantir que as threads saem
        Thread.Sleep(300);

        if (ligado && writer != null && reader != null)
        {
            lock (lockSocket)
            {
                try
                {
                    string discMsg = $"DISCONNECT|{sensorId}";
                    Console.WriteLine($"[SENSOR] A enviar: {discMsg}");
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