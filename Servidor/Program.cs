using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Microsoft.Data.Sqlite;

const int PORT = 6000;
const string DB_CONNECTION_STRING = "Data Source=OneHealth.db";

ConcurrentDictionary<string, Mutex> fileMutexes = new(StringComparer.OrdinalIgnoreCase);
SemaphoreSlim databaseLock = new(1, 1);

InitializeDatabase();

TcpListener listener = new(IPAddress.Any, PORT);
listener.Start();

Console.WriteLine($"[SERVIDOR] À escuta na porta {PORT}...");

while (true)
{
    TcpClient gatewayClient = await listener.AcceptTcpClientAsync();
    Console.WriteLine($"[SERVIDOR] Gateway ligado: {gatewayClient.Client.RemoteEndPoint}");
    _ = Task.Run(() => HandleGatewayAsync(gatewayClient));
}

async Task HandleGatewayAsync(TcpClient client)
{
    using (client)
    {
        using NetworkStream stream = client.GetStream();
        using StreamReader reader = new(stream, Encoding.UTF8);
        using StreamWriter writer = new(stream, Encoding.UTF8) { AutoFlush = true };

        try
        {
            while (true)
            {
                string? line = await reader.ReadLineAsync();
                if (line == null)
                {
                    break;
                }

                Console.WriteLine($"[SERVIDOR] Recebido: {line}");

                if (!line.StartsWith("DATA_BATCH|", StringComparison.OrdinalIgnoreCase))
                {
                    await writer.WriteLineAsync("SERVER_ACK|ERRO|COMANDO_DESCONHECIDO");
                    continue;
                }

                string[] mainParts = line.Split('|', 4);
                if (mainParts.Length < 4 || !int.TryParse(mainParts[2], out int expectedCount))
                {
                    await writer.WriteLineAsync("LOTE_ACK|ERRO|FORMATO_INVALIDO");
                    continue;
                }

                string gatewayId = mainParts[1].Trim();
                string[] medicoes = mainParts[3]
                    .Split('#', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

                if (medicoes.Length != expectedCount)
                {
                    await writer.WriteLineAsync("LOTE_ACK|ERRO|CONTAGEM_INVALIDA");
                    continue;
                }

                bool success = true;

                foreach (string medicao in medicoes)
                {
                    string[] p = medicao.Split('|');
                    if (p.Length < 5 || !p[0].Equals("DATA", StringComparison.OrdinalIgnoreCase))
                    {
                        success = false;
                        break;
                    }

                    try
                    {
                        await GravarDadosAsync(gatewayId, p[1], p[2], p[3], p[4]);
                    }
                    catch (Exception ex)
                    {
                        success = false;
                        Console.WriteLine($"[SERVIDOR] Erro ao gravar medição: {ex.Message}");
                        break;
                    }
                }

                await writer.WriteLineAsync(success ? "LOTE_ACK|SUCESSO" : "LOTE_ACK|ERRO|PROCESSAMENTO");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[SERVIDOR] Erro: {ex.Message}");
        }
    }

    Console.WriteLine("[SERVIDOR] Gateway desligado.");
}

async Task GravarDadosAsync(string gatewayId, string sensorId, string timestamp, string tipoDado, string valor)
{
    string normalizedType = tipoDado.Trim().ToUpperInvariant();
    string fileName = $"{normalizedType}.txt";
    Mutex fileMutex = fileMutexes.GetOrAdd(fileName, _ => new Mutex());

    fileMutex.WaitOne();
    try
    {
        string logEntry = $"{timestamp} | Gateway: {gatewayId} | Sensor: {sensorId} | Valor: {valor}";
        File.AppendAllText(fileName, logEntry + Environment.NewLine);
        Console.WriteLine($"[FICHEIRO] {fileName} atualizado.");
    }
    finally
    {
        fileMutex.ReleaseMutex();
    }

    await databaseLock.WaitAsync();
    try
    {
        using SqliteConnection connection = new(DB_CONNECTION_STRING);
        connection.Open();

        using SqliteCommand command = connection.CreateCommand();
        command.CommandText = @"
            INSERT INTO Medicoes (GatewayId, SensorId, TipoDado, Valor, Timestamp)
            VALUES ($gatewayId, $sensorId, $tipoDado, $valor, $timestamp)";

        command.Parameters.AddWithValue("$gatewayId", gatewayId.Trim());
        command.Parameters.AddWithValue("$sensorId", sensorId.Trim());
        command.Parameters.AddWithValue("$tipoDado", normalizedType);
        command.Parameters.AddWithValue("$valor", valor.Trim());
        command.Parameters.AddWithValue("$timestamp", timestamp.Trim());

        command.ExecuteNonQuery();
    }
    finally
    {
        databaseLock.Release();
    }
}

void InitializeDatabase()
{
    using SqliteConnection connection = new(DB_CONNECTION_STRING);
    connection.Open();

    using SqliteCommand command = connection.CreateCommand();
    command.CommandText = @"
        CREATE TABLE IF NOT EXISTS Medicoes (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            GatewayId TEXT NOT NULL,
            SensorId TEXT NOT NULL,
            TipoDado TEXT NOT NULL,
            Valor TEXT NOT NULL,
            Timestamp TEXT NOT NULL
        )";
    command.ExecuteNonQuery();

    Console.WriteLine("[SERVIDOR] Base de dados pronta.");
}
