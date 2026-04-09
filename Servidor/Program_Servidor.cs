using System.Net;
using System.Net.Sockets;
using System.Text;

const int PORT = 6000;
const string LOG_FILE = "server_log.txt";

TcpListener listener = new TcpListener(IPAddress.Any, PORT);
listener.Start();

Console.WriteLine($"[SERVIDOR] À escuta na porta {PORT}...");

while (true)
{
    TcpClient gatewayClient = await listener.AcceptTcpClientAsync();
    Console.WriteLine("[SERVIDOR] Gateway ligado.");
    _ = Task.Run(() => HandleGatewayAsync(gatewayClient));
}

static async Task HandleGatewayAsync(TcpClient client)
{
    using (client)
    using (NetworkStream stream = client.GetStream())
    using (StreamReader reader = new StreamReader(stream, Encoding.UTF8))
    using (StreamWriter writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true })
    {
        try
        {
            while (true)
            {
                string? line = await reader.ReadLineAsync();
                if (line == null) break;

                string log = $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {line}";
                Console.WriteLine($"[SERVIDOR] {log}");
                File.AppendAllText(LOG_FILE, log + Environment.NewLine);

                await writer.WriteLineAsync("SERVER_ACK|SUCESSO");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[SERVIDOR] Erro: {ex.Message}");
        }
    }

    Console.WriteLine("[SERVIDOR] Gateway desligado.");
}