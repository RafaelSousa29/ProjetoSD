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

                if (line.StartsWith("DATA_BATCH|", StringComparison.OrdinalIgnoreCase))
                {
                    await HandleDataBatchAsync(line, reader, writer);
                    continue;
                }

                if (line.StartsWith("VIDEO_BATCH|", StringComparison.OrdinalIgnoreCase))
                {
                    await HandleVideoBatchAsync(line, reader, writer);
                    continue;
                }

                await writer.WriteLineAsync("SERVER_ACK|ERRO|COMANDO_DESCONHECIDO");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[SERVIDOR] Erro: {ex.Message}");
        }
    }

    Console.WriteLine("[SERVIDOR] Gateway desligado.");
}

async Task HandleDataBatchAsync(string header, StreamReader reader, StreamWriter writer)
{
    string[] mainParts = header.Split('|', 3);
    if (mainParts.Length < 3 || !int.TryParse(mainParts[2], out int expectedCount) || expectedCount < 0)
    {
        await writer.WriteLineAsync("BATCH_ACK|ERRO|FORMATO_INVALIDO");
        return;
    }

    string gatewayId = mainParts[1].Trim();
    (List<string> medicoes, bool terminatorReceived) = await ReadBatchLinesAsync(reader, expectedCount);
    if (!terminatorReceived || medicoes.Count != expectedCount)
    {
        await writer.WriteLineAsync("BATCH_ACK|ERRO|CONTAGEM_INVALIDA");
        return;
    }

    bool success = true;
    Dictionary<string, int> updatedTypes = new(StringComparer.OrdinalIgnoreCase);

    Console.WriteLine($"[SERVIDOR] Lote recebido de {gatewayId}: {expectedCount} medições.");

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
            string normalizedType = p[3].Trim().ToUpperInvariant();
            updatedTypes[normalizedType] = updatedTypes.GetValueOrDefault(normalizedType) + 1;
        }
        catch (Exception ex)
        {
            success = false;
            Console.WriteLine($"[SERVIDOR] Erro ao gravar medição: {ex.Message}");
            break;
        }
    }

    if (success)
    {
        foreach (KeyValuePair<string, int> entry in updatedTypes)
        {
            Console.WriteLine($"[SERVIDOR] {entry.Key}.txt atualizado ({entry.Value} registos).");
        }
    }

    await writer.WriteLineAsync(success ? "BATCH_ACK|SUCESSO" : "BATCH_ACK|ERRO|PROCESSAMENTO");
}

async Task HandleVideoBatchAsync(string header, StreamReader reader, StreamWriter writer)
{
    string[] mainParts = header.Split('|', 4);
    if (mainParts.Length < 4 || !int.TryParse(mainParts[3], out int expectedCount) || expectedCount < 0)
    {
        await writer.WriteLineAsync("VIDEO_BATCH_ACK|ERRO|FORMATO_INVALIDO");
        return;
    }

    string gatewayId = mainParts[1].Trim();
    string sensorId = mainParts[2].Trim();
    (List<string> videoLines, bool terminatorReceived) = await ReadBatchLinesAsync(reader, expectedCount);
    if (!terminatorReceived || videoLines.Count != expectedCount)
    {
        await writer.WriteLineAsync("VIDEO_BATCH_ACK|ERRO|CONTAGEM_INVALIDA");
        return;
    }

    try
    {
        await GravarVideoAsync(gatewayId, sensorId, videoLines);
        Console.WriteLine($"[SERVIDOR] Vídeo recebido de {gatewayId}/{sensorId}: {videoLines.Count} linhas.");
        await writer.WriteLineAsync("VIDEO_BATCH_ACK|SUCESSO");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[SERVIDOR] Erro ao gravar vídeo: {ex.Message}");
        await writer.WriteLineAsync("VIDEO_BATCH_ACK|ERRO|PROCESSAMENTO");
    }
}

async Task<(List<string> Lines, bool TerminatorReceived)> ReadBatchLinesAsync(StreamReader reader, int expectedCount)
{
    List<string> lines = new(expectedCount);
    bool terminatorReceived = false;

    while (true)
    {
        string? line = await reader.ReadLineAsync();
        if (line == null)
        {
            break;
        }

        if (line.Equals("END", StringComparison.OrdinalIgnoreCase))
        {
            terminatorReceived = true;
            break;
        }

        lines.Add(line);
    }

    return (lines, terminatorReceived);
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

async Task GravarVideoAsync(string gatewayId, string sensorId, List<string> videoLines)
{
    string fileName = $"VIDEO_{sensorId}.txt";
    Mutex fileMutex = fileMutexes.GetOrAdd(fileName, _ => new Mutex());

    fileMutex.WaitOne();
    try
    {
        List<string> logLines = new();
        logLines.Add($"--- VIDEO BATCH | Gateway: {gatewayId} | Sensor: {sensorId} | Recebido: {DateTime.Now:yyyy-MM-ddTHH:mm:ss} ---");
        logLines.AddRange(videoLines);
        File.AppendAllLines(fileName, logLines);
    }
    finally
    {
        fileMutex.ReleaseMutex();
    }

    await Task.CompletedTask;
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
