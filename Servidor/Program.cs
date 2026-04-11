using System.Net;
using System.Net.Sockets;
using System.Text;

const int PORT = 6000;
const string LOG_FILE = "server_log.txt";

object lockLog = new object();
bool servidorEmExecucao = true;

TcpListener listener = new TcpListener(IPAddress.Any, PORT);
listener.Start();

Console.WriteLine($"[SERVIDOR] À escuta na porta {PORT}...");
Console.WriteLine("[SERVIDOR] Escreva 'stop' para encerrar.");

// Thread para escuta de comandos
Thread commandThread = new Thread(ListenarComandos)
{
    Name = "CommandListener",
    IsBackground = false
};
commandThread.Start();

// Loop principal - Aceitar conexões de gateways
while (servidorEmExecucao)
{
    try
    {
        TcpClient gatewayClient = listener.AcceptTcpClient();
        Console.WriteLine("[SERVIDOR] Gateway ligado.");
        
        // Criar thread para cada gateway
        Thread gatewayThread = new Thread(HandleGateway)
        {
            Name = $"Gateway-{DateTime.Now:HHmmss}",
            IsBackground = true
        };
        gatewayThread.Start(gatewayClient);
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[SERVIDOR] Erro ao aceitar conexão: {ex.Message}");
    }
}

return;

void HandleGateway(object? clientObj)
{
    if (clientObj is not TcpClient client)
        return;

    using (client)
    using (NetworkStream stream = client.GetStream())
    using (StreamReader reader = new StreamReader(stream, Encoding.UTF8))
    using (StreamWriter writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true })
    {
        try
        {
            while (servidorEmExecucao)
            {
                string? line = reader.ReadLine();
                if (line == null) break;

                string log = $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {line}";
                Console.WriteLine($"[SERVIDOR] {log}");
                
                lock (lockLog)
                {
                    try
                    {
                        File.AppendAllText(LOG_FILE, log + Environment.NewLine);
                    }
                    catch { }
                }

                writer.WriteLine("SERVER_ACK|SUCESSO");
                writer.Flush();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[SERVIDOR] Erro: {ex.Message}");
        }
    }

    Console.WriteLine("[SERVIDOR] Gateway desligado.");
}

void ListenarComandos()
{
    while (servidorEmExecucao)
    {
        try
        {
            string? cmd = Console.ReadLine();
            if (cmd?.ToLower() == "stop")
            {
                Console.WriteLine("[SERVIDOR] A encerrar...");
                servidorEmExecucao = false;
                break;
            }
        }
        catch { }
    }
}