using System.Net.Sockets;
using System.Text;
using System.Threading;

const string GATEWAY_IP = "127.0.0.1";
const int GATEWAY_PORT = 5000;

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
int heartbeatCount = 0;

SemaphoreSlim socketLock = new SemaphoreSlim(1, 1);

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
        Console.WriteLine("[SENSOR] Ligação aceite em modo normal.");

        heartbeatAtivo = true;
        _ = Task.Run(HeartbeatLoop);
    }
    else if (response == "CONN_ACK|MANUTENCAO")
    {
        ligado = true;
        modoManutencao = true;
        Console.WriteLine("[SENSOR] Ligação aceite em modo manutenção.");
        Console.WriteLine("[SENSOR] O heartbeat automático fica desativado.");
    }
    else
    {
        Console.WriteLine("[SENSOR] Ligação recusada. O programa vai terminar.");
        reader.Dispose();
        writer.Dispose();
        client.Dispose();
        return;
    }
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

    if (!modoManutencao)
    {
        Console.WriteLine("1 - Enviar medição");
        Console.WriteLine("2 - Desligar");
        Console.Write("Opção: ");
        string? opcao = Console.ReadLine();

        try
        {
            switch (opcao)
            {
                case "1":
                    await EnviarMedicao();
                    break;

                case "2":
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
    else
    {
        Console.WriteLine("1 - Enviar medição");
        Console.WriteLine("2 - Enviar heartbeat manual");
        Console.WriteLine("3 - Desligar");
        Console.Write("Opção: ");
        string? opcao = Console.ReadLine();

        try
        {
            switch (opcao)
            {
                case "1":
                    await EnviarMedicao();
                    break;

                case "2":
                    await EnviarHeartbeatManual();
                    break;

                case "3":
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
}

async Task EnviarMedicao()
{
    if (!ligado || writer == null || reader == null)
    {
        Console.WriteLine("O sensor não está ligado ao gateway.");
        return;
    }

    Console.Write("Tipo de dado: ");
    string tipo = Console.ReadLine()?.Trim() ?? "TEMP";

    Console.Write("Valor: ");
    string valor = Console.ReadLine()?.Trim() ?? "0";

    string timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");

    await socketLock.WaitAsync();
    try
    {
        string dataMsg = $"DATA|{sensorId}|{timestamp}|{tipo}|{valor}";
        await writer.WriteLineAsync(dataMsg);

        string? response = await reader.ReadLineAsync();
        Console.WriteLine($"[SENSOR] Resposta: {response}");
    }
    finally
    {
        socketLock.Release();
    }
}

async Task EnviarHeartbeatManual()
{
    if (!ligado || writer == null || reader == null)
    {
        Console.WriteLine("O sensor não está ligado ao gateway.");
        return;
    }

    await socketLock.WaitAsync();
    try
    {
        string timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");
        string hbMsg = $"HEARTBEAT|{sensorId}|{timestamp}";
        await writer.WriteLineAsync(hbMsg);

        string? response = await reader.ReadLineAsync();
        Console.WriteLine($"[SENSOR] Resposta: {response}");
    }
    finally
    {
        socketLock.Release();
    }
}

async Task HeartbeatLoop()
{
    while (heartbeatAtivo)
    {
        try
        {
            await Task.Delay(10000);

            if (!heartbeatAtivo || !ligado || writer == null || reader == null)
                continue;

            await socketLock.WaitAsync();
            try
            {
                string timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");
                string hbMsg = $"HEARTBEAT|{sensorId}|{timestamp}";
                await writer.WriteLineAsync(hbMsg);

                string? response = await reader.ReadLineAsync();
                heartbeatCount++;

                if (response == null || !response.StartsWith("ACK_HEARTBEAT|SUCESSO"))
                {
                    Console.WriteLine($"[SENSOR] Erro no heartbeat: {response}");
                }
                else if (heartbeatCount % 10 == 0)
                {
                    Console.WriteLine($"[SENSOR] {heartbeatCount} heartbeats enviados com sucesso.");
                }
            }
            finally
            {
                socketLock.Release();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[SENSOR] Erro no heartbeat: {ex.Message}");
            break;
        }
    }
}

async Task DesligarSensor()
{
    try
    {
        heartbeatAtivo = false;

        // Pequena pausa para garantir que o loop de heartbeat sai
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

    try { reader?.Dispose(); } catch { }
    try { writer?.Dispose(); } catch { }
    try { client?.Dispose(); } catch { }

    reader = null;
    writer = null;
    client = null;

    await Task.CompletedTask;
}