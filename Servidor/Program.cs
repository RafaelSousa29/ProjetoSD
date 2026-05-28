using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;

const int PORT = 6000;
const int DASHBOARD_PORT = 8080;
const string DB_CONNECTION_STRING = "Data Source=OneHealth.db";
const string ANALYSIS_RPC_IP = "127.0.0.1";
const int ANALYSIS_RPC_PORT = 7001;

ConcurrentDictionary<string, Mutex> fileMutexes = new(StringComparer.OrdinalIgnoreCase);
SemaphoreSlim databaseLock = new(1, 1);
JsonSerializerOptions jsonOptions = new(JsonSerializerDefaults.Web);

InitializeDatabase();

TcpListener listener = new(IPAddress.Any, PORT);
listener.Start();
Console.WriteLine("[SERVIDOR] Comandos: 'analises', 'medicoes', 'reanalyze', 'help'.");
_ = Task.Run(ServerCommandLoopAsync);
_ = Task.Run(DashboardLoopAsync);
Console.WriteLine($"[DASHBOARD] Disponivel em http://localhost:{DASHBOARD_PORT}");

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

                if (line.StartsWith("DATA_BATCH_JSON|", StringComparison.OrdinalIgnoreCase))
                {
                    await HandleStructuredDataBatchAsync(line, reader, writer);
                    continue;
                }

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

    bool success = await ProcessMeasurementBatchAsync(gatewayId, medicoes, $"texto:{expectedCount}");

    await writer.WriteLineAsync(success ? "BATCH_ACK|SUCESSO" : "BATCH_ACK|ERRO|PROCESSAMENTO");
}

async Task HandleStructuredDataBatchAsync(string header, StreamReader reader, StreamWriter writer)
{
    string[] mainParts = header.Split('|', 3);
    if (mainParts.Length < 3 || !int.TryParse(mainParts[2], out int expectedJsonLines) || expectedJsonLines <= 0)
    {
        await writer.WriteLineAsync("BATCH_ACK|ERRO|FORMATO_INVALIDO");
        return;
    }

    string gatewayId = mainParts[1].Trim();
    (List<string> jsonLines, bool terminatorReceived) = await ReadBatchLinesAsync(reader, expectedJsonLines);
    if (!terminatorReceived || jsonLines.Count != expectedJsonLines)
    {
        await writer.WriteLineAsync("BATCH_ACK|ERRO|CONTAGEM_INVALIDA");
        return;
    }

    try
    {
        string json = string.Join("", jsonLines);
        StructuredDataBatch? structuredBatch = JsonSerializer.Deserialize<StructuredDataBatch>(json, jsonOptions);
        if (structuredBatch == null ||
            structuredBatch.Measurements == null ||
            !string.Equals(structuredBatch.GatewayId, gatewayId, StringComparison.OrdinalIgnoreCase) ||
            structuredBatch.Measurements.Count != structuredBatch.TotalRecords)
        {
            await writer.WriteLineAsync("BATCH_ACK|ERRO|ESTRUTURA_INVALIDA");
            return;
        }

        List<string> medicoes = structuredBatch.Measurements
            .Select(measurement => $"DATA|{measurement.SensorId}|{measurement.Timestamp}|{measurement.Type}|{measurement.Value}")
            .ToList();

        Console.WriteLine($"[SERVIDOR] Estrutura recebida de {gatewayId}: batch={structuredBatch.BatchId}, sentAt={structuredBatch.SentAt}, total={structuredBatch.TotalRecords}.");
        LogStructuredSummary(structuredBatch);

        bool success = await ProcessMeasurementBatchAsync(gatewayId, medicoes, $"json:{structuredBatch.BatchId}");
        await writer.WriteLineAsync(success ? "BATCH_ACK|SUCESSO" : "BATCH_ACK|ERRO|PROCESSAMENTO");
    }
    catch (JsonException ex)
    {
        Console.WriteLine($"[SERVIDOR] JSON de lote inválido: {ex.Message}");
        await writer.WriteLineAsync("BATCH_ACK|ERRO|JSON_INVALIDO");
    }
}

async Task<bool> ProcessMeasurementBatchAsync(string gatewayId, List<string> medicoes, string source)
{
    bool success = true;
    Dictionary<string, int> updatedTypes = new(StringComparer.OrdinalIgnoreCase);
    List<string> newMeasurements = new();
    int duplicateCount = 0;

    Console.WriteLine($"[SERVIDOR] Lote recebido de {gatewayId}: {medicoes.Count} medições ({source}).");

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
            bool inserted = await GravarDadosAsync(gatewayId, p[1], p[2], p[3], p[4]);
            if (!inserted)
            {
                duplicateCount++;
                continue;
            }

            string normalizedType = p[3].Trim().ToUpperInvariant();
            updatedTypes[normalizedType] = updatedTypes.GetValueOrDefault(normalizedType) + 1;
            newMeasurements.Add(medicao);
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
        Console.WriteLine($"[SERVIDOR] Lote processado de {gatewayId}: {newMeasurements.Count} novas, {duplicateCount} duplicadas ignoradas.");

        foreach (KeyValuePair<string, int> entry in updatedTypes)
        {
            Console.WriteLine($"[SERVIDOR] {entry.Key}.txt atualizado ({entry.Value} registos).");
        }

        if (newMeasurements.Count > 0)
        {
            await GravarAnalisesAutomaticasPorSensorAsync(gatewayId, newMeasurements);
        }
        else
        {
            Console.WriteLine("[SERVIDOR] Análise RPC ignorada: lote sem medições novas.");
        }
    }

    return success;
}

async Task GravarAnalisesAutomaticasPorSensorAsync(string gatewayId, List<string> newMeasurements)
{
    Dictionary<string, List<string>> measurementsBySensor = new(StringComparer.OrdinalIgnoreCase);

    foreach (string measurement in newMeasurements)
    {
        string[] parts = measurement.Split('|');
        if (parts.Length < 5 || !parts[0].Equals("DATA", StringComparison.OrdinalIgnoreCase))
        {
            continue;
        }

        string sensorId = parts[1].Trim().ToUpperInvariant();
        if (sensorId.Length == 0)
        {
            continue;
        }

        if (!measurementsBySensor.TryGetValue(sensorId, out List<string>? sensorMeasurements))
        {
            sensorMeasurements = new List<string>();
            measurementsBySensor[sensorId] = sensorMeasurements;
        }

        sensorMeasurements.Add(measurement);
    }

    if (measurementsBySensor.Count == 0)
    {
        Console.WriteLine("[SERVIDOR] Análise RPC ignorada: lote sem sensores válidos.");
        return;
    }

    foreach (KeyValuePair<string, List<string>> entry in measurementsBySensor.OrderBy(item => item.Key))
    {
        AnalysisRpcResult analysis = await RequestAnalysisAsync(gatewayId, entry.Value);
        if (analysis.Success)
        {
            await GravarAnaliseAsync(analysis, entry.Key, "TODOS");
            Console.WriteLine($"[SERVIDOR] Análise RPC guardada para {entry.Key}: n={analysis.TotalRecords}, risco={analysis.RiskLevel}, média={analysis.AverageValue}.");
        }
        else
        {
            Console.WriteLine($"[SERVIDOR] Análise RPC não disponível para {entry.Key}: {analysis.Error}");
        }
    }
}

void LogStructuredSummary(StructuredDataBatch structuredBatch)
{
    if (structuredBatch.SummaryByType == null || structuredBatch.SummaryByType.Count == 0)
    {
        return;
    }

    string summary = string.Join("; ", structuredBatch.SummaryByType.Select(entry =>
        $"{entry.Key}:n={entry.Value.Count},min={entry.Value.Min},max={entry.Value.Max},avg={entry.Value.Avg}"));
    Console.WriteLine($"[SERVIDOR] Resumo agregado recebido: {summary}");
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

async Task<bool> GravarDadosAsync(string gatewayId, string sensorId, string timestamp, string tipoDado, string valor)
{
    string normalizedType = tipoDado.Trim().ToUpperInvariant();
    string normalizedSensorId = sensorId.Trim();
    string normalizedTimestamp = timestamp.Trim();
    string normalizedValue = valor.Trim();
    string fileName = $"{normalizedType}.txt";

    await databaseLock.WaitAsync();
    try
    {
        using SqliteConnection connection = new(DB_CONNECTION_STRING);
        connection.Open();

        using SqliteCommand command = connection.CreateCommand();
        command.CommandText = @"
            SELECT 1
            FROM Medicoes
            WHERE SensorId = $sensorId
              AND Timestamp = $timestamp
              AND TipoDado = $tipoDado
            LIMIT 1";

        command.Parameters.AddWithValue("$sensorId", normalizedSensorId);
        command.Parameters.AddWithValue("$timestamp", normalizedTimestamp);
        command.Parameters.AddWithValue("$tipoDado", normalizedType);

        object? existing = command.ExecuteScalar();
        if (existing != null)
        {
            return false;
        }

        command.Parameters.Clear();
        command.CommandText = @"
            INSERT INTO Medicoes (GatewayId, SensorId, TipoDado, Valor, Timestamp)
            VALUES ($gatewayId, $sensorId, $tipoDado, $valor, $timestamp)";

        command.Parameters.AddWithValue("$gatewayId", gatewayId.Trim());
        command.Parameters.AddWithValue("$sensorId", normalizedSensorId);
        command.Parameters.AddWithValue("$tipoDado", normalizedType);
        command.Parameters.AddWithValue("$valor", normalizedValue);
        command.Parameters.AddWithValue("$timestamp", normalizedTimestamp);

        command.ExecuteNonQuery();
    }
    finally
    {
        databaseLock.Release();
    }

    Mutex fileMutex = fileMutexes.GetOrAdd(fileName, _ => new Mutex());

    fileMutex.WaitOne();
    try
    {
        string logEntry = $"{normalizedTimestamp} | Gateway: {gatewayId} | Sensor: {normalizedSensorId} | Valor: {normalizedValue}";
        File.AppendAllText(fileName, logEntry + Environment.NewLine);
    }
    finally
    {
        fileMutex.ReleaseMutex();
    }

    return true;
}

async Task<AnalysisRpcResult> RequestAnalysisAsync(string gatewayId, List<string> measurements)
{
    try
    {
        using TcpClient rpcClient = new();
        await rpcClient.ConnectAsync(ANALYSIS_RPC_IP, ANALYSIS_RPC_PORT).WaitAsync(TimeSpan.FromSeconds(3));

        using NetworkStream stream = rpcClient.GetStream();
        using StreamWriter writer = new(stream, Encoding.UTF8) { AutoFlush = true };
        using StreamReader reader = new(stream, Encoding.UTF8);

        await writer.WriteLineAsync($"ANALYZE_BATCH|{gatewayId}|{measurements.Count}");
        foreach (string measurement in measurements)
        {
            await writer.WriteLineAsync(measurement);
        }
        await writer.WriteLineAsync("END");

        string? response = await reader.ReadLineAsync().WaitAsync(TimeSpan.FromSeconds(5));
        if (response == null)
        {
            return AnalysisRpcResult.Fail("SEM_RESPOSTA_RPC");
        }

        string[] parts = response.Split('|');
        if (parts.Length < 2 || !parts[0].Equals("ANALYZE_ACK", StringComparison.OrdinalIgnoreCase))
        {
            return AnalysisRpcResult.Fail("RESPOSTA_RPC_INVALIDA");
        }

        if (!parts[1].Equals("SUCESSO", StringComparison.OrdinalIgnoreCase))
        {
            return AnalysisRpcResult.Fail(parts.Length >= 3 ? parts[2] : "ERRO_RPC");
        }

        if (parts.Length < 9)
        {
            return AnalysisRpcResult.Fail("RESPOSTA_RPC_INCOMPLETA");
        }

        return AnalysisRpcResult.Ok(
            parts[2].Trim(),
            parts[3].Trim(),
            parts[4].Trim(),
            parts[5].Trim(),
            parts[6].Trim(),
            parts[7].Trim(),
            parts[8].Trim());
    }
    catch (Exception ex)
    {
        return AnalysisRpcResult.Fail($"RPC_INDISPONIVEL:{ex.Message}");
    }
}

async Task GravarAnaliseAsync(AnalysisRpcResult analysis, string sensorId, string tipoDado)
{
    await databaseLock.WaitAsync();
    try
    {
        using SqliteConnection connection = new(DB_CONNECTION_STRING);
        connection.Open();

        using SqliteCommand command = connection.CreateCommand();
        command.CommandText = @"
            INSERT INTO Analises (GatewayId, SensorId, TipoDado, GeneratedAt, TotalRecords, AverageValue, MaxValue, RiskLevel, Summary)
            VALUES ($gatewayId, $sensorId, $tipoDado, $generatedAt, $totalRecords, $averageValue, $maxValue, $riskLevel, $summary)";

        command.Parameters.AddWithValue("$gatewayId", analysis.GatewayId);
        command.Parameters.AddWithValue("$sensorId", sensorId.Trim().ToUpperInvariant());
        command.Parameters.AddWithValue("$tipoDado", NormalizeAnalysisType(tipoDado));
        command.Parameters.AddWithValue("$generatedAt", analysis.GeneratedAt);
        command.Parameters.AddWithValue("$totalRecords", analysis.TotalRecords);
        command.Parameters.AddWithValue("$averageValue", analysis.AverageValue);
        command.Parameters.AddWithValue("$maxValue", analysis.MaxValue);
        command.Parameters.AddWithValue("$riskLevel", analysis.RiskLevel);
        command.Parameters.AddWithValue("$summary", analysis.Summary);

        command.ExecuteNonQuery();
    }
    finally
    {
        databaseLock.Release();
    }
}

async Task ServerCommandLoopAsync()
{
    while (true)
    {
        string? command = Console.ReadLine();
        if (command == null)
        {
            await Task.Delay(200);
            continue;
        }

        command = command.Trim();
        if (command.Length == 0)
        {
            continue;
        }

        try
        {
            if (command.Equals("help", StringComparison.OrdinalIgnoreCase))
            {
                PrintServerCommands();
            }
            else if (command.Equals("analises", StringComparison.OrdinalIgnoreCase))
            {
                await PrintLatestAnalysesAsync();
            }
            else if (command.Equals("medicoes", StringComparison.OrdinalIgnoreCase))
            {
                await PrintLatestMeasurementsAsync();
            }
            else if (command.Equals("reanalyze", StringComparison.OrdinalIgnoreCase))
            {
                await RunManualAnalysisAsync();
            }
            else
            {
                Console.WriteLine("[SERVIDOR] Comando desconhecido. Use 'help'.");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[SERVIDOR] Erro no comando: {ex.Message}");
        }
    }
}

void PrintServerCommands()
{
    Console.WriteLine("[SERVIDOR] Comandos disponiveis:");
    Console.WriteLine("  analises  - lista as ultimas analises RPC guardadas");
    Console.WriteLine("  medicoes  - lista as ultimas medicoes recebidas");
    Console.WriteLine("  reanalyze - pede nova analise RPC com medicoes ja guardadas");
}

async Task PrintLatestAnalysesAsync()
{
    await databaseLock.WaitAsync();
    try
    {
        using SqliteConnection connection = new(DB_CONNECTION_STRING);
        connection.Open();

        using SqliteCommand command = connection.CreateCommand();
        command.CommandText = @"
            SELECT Id, SensorId, GeneratedAt, TotalRecords, TipoDado, AverageValue, MaxValue, RiskLevel, Summary
            FROM Analises
            ORDER BY Id DESC
            LIMIT 10";

        using SqliteDataReader reader = command.ExecuteReader();
        if (!reader.HasRows)
        {
            Console.WriteLine("[SERVIDOR] Ainda nao existem analises guardadas.");
            return;
        }

        Console.WriteLine("[SERVIDOR] Ultimas analises:");
        while (reader.Read())
        {
            Console.WriteLine(
                $"  #{reader.GetInt32(0)} | Sensor={reader.GetString(1)} | {reader.GetString(2)} | " +
                $"n={reader.GetString(3)} | tipo={reader.GetString(4)} | media={reader.GetString(5)} | " +
                $"max={reader.GetString(6)} | risco={reader.GetString(7)} | {reader.GetString(8)}");
        }
    }
    finally
    {
        databaseLock.Release();
    }
}

async Task PrintLatestMeasurementsAsync()
{
    await databaseLock.WaitAsync();
    try
    {
        using SqliteConnection connection = new(DB_CONNECTION_STRING);
        connection.Open();

        using SqliteCommand command = connection.CreateCommand();
        command.CommandText = @"
            SELECT Id, GatewayId, SensorId, Timestamp, TipoDado, Valor
            FROM Medicoes
            ORDER BY Id DESC
            LIMIT 10";

        using SqliteDataReader reader = command.ExecuteReader();
        if (!reader.HasRows)
        {
            Console.WriteLine("[SERVIDOR] Ainda nao existem medicoes guardadas.");
            return;
        }

        Console.WriteLine("[SERVIDOR] Ultimas medicoes:");
        while (reader.Read())
        {
            Console.WriteLine(
                $"  #{reader.GetInt32(0)} | GW={reader.GetString(1)} | Sensor={reader.GetString(2)} | " +
                $"{reader.GetString(3)} | {reader.GetString(4)}={reader.GetString(5)}");
        }
    }
    finally
    {
        databaseLock.Release();
    }
}

async Task RunManualAnalysisAsync()
{
    Console.Write("Sensor para analisar (ex: S101): ");
    string sensorId = Console.ReadLine()?.Trim().ToUpperInvariant() ?? "";
    if (sensorId.Length == 0)
    {
        Console.WriteLine("[SERVIDOR] Sensor obrigatorio para gerar a analise.");
        return;
    }

    Console.Write("Tipo de dado (ENTER = todos, ex: TEMP): ");
    string tipoDado = NormalizeAnalysisType(Console.ReadLine()?.Trim() ?? "");

    Console.Write("Numero de medicoes recentes (ENTER = 20): ");
    string countText = Console.ReadLine()?.Trim() ?? "";
    if (!int.TryParse(countText, out int count) || count <= 0)
    {
        count = 20;
    }

    List<string> measurements = await GetLatestMeasurementsForAnalysisAsync(sensorId, tipoDado, count);
    if (measurements.Count == 0)
    {
        Console.WriteLine($"[SERVIDOR] Nao existem medicoes para {sensorId}/{tipoDado}.");
        return;
    }

    AnalysisRpcResult analysis = await RequestAnalysisAsync(BuildAnalysisScope(sensorId, tipoDado), measurements);
    if (!analysis.Success)
    {
        Console.WriteLine($"[SERVIDOR] Analise manual falhou: {analysis.Error}");
        return;
    }

    await GravarAnaliseAsync(analysis, sensorId, tipoDado);
    Console.WriteLine(
        $"[SERVIDOR] Analise manual guardada para {sensorId}/{tipoDado}: n={analysis.TotalRecords}, risco={analysis.RiskLevel}, media={analysis.AverageValue}.");
}

string NormalizeAnalysisType(string tipoDado)
{
    string normalized = tipoDado.Trim().ToUpperInvariant();
    return normalized.Length == 0 || normalized == "ALL" || normalized == "*" ? "TODOS" : normalized;
}

string BuildAnalysisScope(string sensorId, string tipoDado)
{
    string normalizedType = NormalizeAnalysisType(tipoDado);
    return normalizedType == "TODOS" ? sensorId.Trim().ToUpperInvariant() : $"{sensorId.Trim().ToUpperInvariant()}/{normalizedType}";
}

async Task<List<string>> GetLatestMeasurementsForAnalysisAsync(string sensorId, string tipoDado, int count)
{
    List<MeasurementRow> rows = new();
    string normalizedSensorId = sensorId.Trim().ToUpperInvariant();
    string normalizedType = NormalizeAnalysisType(tipoDado);

    await databaseLock.WaitAsync();
    try
    {
        using SqliteConnection connection = new(DB_CONNECTION_STRING);
        connection.Open();

        using SqliteCommand command = connection.CreateCommand();
        command.CommandText = @"
            SELECT SensorId, Timestamp, TipoDado, Valor
            FROM Medicoes
            WHERE SensorId = $sensorId
              AND ($tipoDado = 'TODOS' OR TipoDado = $tipoDado)
            ORDER BY Id DESC
            LIMIT $count";
        command.Parameters.AddWithValue("$sensorId", normalizedSensorId);
        command.Parameters.AddWithValue("$tipoDado", normalizedType);
        command.Parameters.AddWithValue("$count", count);

        using SqliteDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            rows.Add(new MeasurementRow(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetString(3)));
        }
    }
    finally
    {
        databaseLock.Release();
    }

    rows.Reverse();
    return rows
        .Select(row => $"DATA|{row.SensorId}|{row.Timestamp}|{row.TipoDado}|{row.Valor}")
        .ToList();
}

async Task DashboardLoopAsync()
{
    using HttpListener http = new();
    http.Prefixes.Add($"http://localhost:{DASHBOARD_PORT}/");

    try
    {
        http.Start();
        while (true)
        {
            HttpListenerContext context = await http.GetContextAsync();
            _ = Task.Run(() => HandleDashboardRequestAsync(context));
        }
    }
    catch (HttpListenerException ex)
    {
        Console.WriteLine($"[DASHBOARD] Nao foi possivel iniciar: {ex.Message}");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[DASHBOARD] Erro: {ex.Message}");
    }
}

async Task HandleDashboardRequestAsync(HttpListenerContext context)
{
    string path = context.Request.Url?.AbsolutePath.TrimEnd('/') ?? "";
    if (path.Length == 0)
    {
        path = "/";
    }

    try
    {
        if (path.Equals("/", StringComparison.OrdinalIgnoreCase))
        {
            await WriteResponseAsync(context.Response, GetDashboardHtml(), "text/html; charset=utf-8");
            return;
        }

        if (path.Equals("/api/dashboard", StringComparison.OrdinalIgnoreCase))
        {
            DashboardPayload payload = await ReadDashboardPayloadAsync();
            string json = JsonSerializer.Serialize(payload, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                WriteIndented = false
            });
            await WriteResponseAsync(context.Response, json, "application/json; charset=utf-8");
            return;
        }

        if (path.Equals("/api/reanalyze", StringComparison.OrdinalIgnoreCase))
        {
            string sensorId = context.Request.QueryString["sensorId"]?.Trim().ToUpperInvariant() ?? "";
            string tipoDado = NormalizeAnalysisType(context.Request.QueryString["tipoDado"]?.Trim() ?? "");

            if (!int.TryParse(context.Request.QueryString["count"], out int count) || count <= 0)
            {
                count = 20;
            }

            DashboardActionResult result = await RunDashboardAnalysisAsync(sensorId, tipoDado, count);
            string json = JsonSerializer.Serialize(result, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                WriteIndented = false
            });
            context.Response.StatusCode = result.Success ? 200 : 400;
            await WriteResponseAsync(context.Response, json, "application/json; charset=utf-8");
            return;
        }

        if (path.Equals("/api/clear-data", StringComparison.OrdinalIgnoreCase))
        {
            string confirmation = context.Request.QueryString["confirm"]?.Trim() ?? "";
            DashboardActionResult result = confirmation.Equals("LIMPAR", StringComparison.Ordinal)
                ? await ClearDashboardDataAsync()
                : new DashboardActionResult(false, "Confirmacao invalida. Escreve LIMPAR para apagar os dados.");

            string json = JsonSerializer.Serialize(result, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                WriteIndented = false
            });
            context.Response.StatusCode = result.Success ? 200 : 400;
            await WriteResponseAsync(context.Response, json, "application/json; charset=utf-8");
            return;
        }

        context.Response.StatusCode = 404;
        await WriteResponseAsync(context.Response, "Not found", "text/plain; charset=utf-8");
    }
    catch (Exception ex)
    {
        context.Response.StatusCode = 500;
        await WriteResponseAsync(context.Response, $"Dashboard error: {ex.Message}", "text/plain; charset=utf-8");
    }
}

async Task WriteResponseAsync(HttpListenerResponse response, string content, string contentType)
{
    byte[] bytes = Encoding.UTF8.GetBytes(content);
    response.ContentType = contentType;
    response.ContentLength64 = bytes.Length;
    await response.OutputStream.WriteAsync(bytes);
    response.Close();
}

async Task<DashboardPayload> ReadDashboardPayloadAsync()
{
    DashboardCounters counters;
    List<MeasurementView> measurements = new();
    List<AnalysisView> analyses = new();
    List<VideoStreamView> videoStreams = ReadVideoStreams();
    Dictionary<string, TypeAccumulator> typeAccumulators = new(StringComparer.OrdinalIgnoreCase);
    Dictionary<string, SensorAccumulator> sensorAccumulators = new(StringComparer.OrdinalIgnoreCase);

    await databaseLock.WaitAsync();
    try
    {
        using SqliteConnection connection = new(DB_CONNECTION_STRING);
        connection.Open();

        long totalMeasurements = ReadLong(connection, "SELECT COUNT(*) FROM Medicoes");
        long totalSensors = ReadLong(connection, "SELECT COUNT(DISTINCT SensorId) FROM Medicoes");
        long totalTypes = ReadLong(connection, "SELECT COUNT(DISTINCT TipoDado) FROM Medicoes");
        long totalAnalyses = ReadLong(connection, "SELECT COUNT(*) FROM Analises");
        string lastTimestamp = ReadText(connection, "SELECT Timestamp FROM Medicoes ORDER BY Id DESC LIMIT 1");

        counters = new DashboardCounters(totalMeasurements, totalSensors, totalTypes, totalAnalyses, lastTimestamp);

        using (SqliteCommand command = connection.CreateCommand())
        {
            command.CommandText = @"
                SELECT Id, GatewayId, SensorId, Timestamp, TipoDado, Valor
                FROM Medicoes
                ORDER BY Id DESC
                LIMIT 200";

            using SqliteDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                measurements.Add(new MeasurementView(
                    reader.GetInt32(0),
                    reader.GetString(1),
                    reader.GetString(2),
                    reader.GetString(3),
                    reader.GetString(4),
                    reader.GetString(5)));
            }
        }

        using (SqliteCommand command = connection.CreateCommand())
        {
            command.CommandText = @"
                SELECT Id, SensorId, GeneratedAt, TotalRecords, TipoDado, AverageValue, MaxValue, RiskLevel, Summary
                FROM Analises
                ORDER BY Id DESC
                LIMIT 10";

            using SqliteDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                analyses.Add(new AnalysisView(
                    reader.GetInt32(0),
                    reader.GetString(1),
                    reader.GetString(2),
                    reader.GetString(3),
                    reader.GetString(4),
                    reader.GetString(5),
                    reader.GetString(6),
                    reader.GetString(7),
                    reader.GetString(8)));
            }
        }

        using (SqliteCommand command = connection.CreateCommand())
        {
            command.CommandText = "SELECT SensorId, TipoDado, Valor, Timestamp FROM Medicoes ORDER BY Id ASC";

            using SqliteDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                string sensorId = reader.GetString(0);
                string type = reader.GetString(1);
                string valueText = reader.GetString(2);
                string timestamp = reader.GetString(3);

                if (!typeAccumulators.TryGetValue(type, out TypeAccumulator? typeAccumulator))
                {
                    typeAccumulator = new TypeAccumulator(type);
                    typeAccumulators[type] = typeAccumulator;
                }

                typeAccumulator.Count++;
                if (TryParseDecimal(valueText, out decimal value))
                {
                    typeAccumulator.NumericCount++;
                    typeAccumulator.Sum += value;
                    typeAccumulator.Min = typeAccumulator.Min is null ? value : Math.Min(typeAccumulator.Min.Value, value);
                    typeAccumulator.Max = typeAccumulator.Max is null ? value : Math.Max(typeAccumulator.Max.Value, value);
                }

                if (!sensorAccumulators.TryGetValue(sensorId, out SensorAccumulator? sensorAccumulator))
                {
                    sensorAccumulator = new SensorAccumulator(sensorId);
                    sensorAccumulators[sensorId] = sensorAccumulator;
                }

                sensorAccumulator.Count++;
                sensorAccumulator.LastTimestamp = timestamp;
                sensorAccumulator.Types.Add(type);
            }
        }
    }
    finally
    {
        databaseLock.Release();
    }

    List<TypeStat> typeStats = typeAccumulators.Values
        .OrderByDescending(item => item.Count)
        .Select(item => new TypeStat(
            item.Type,
            item.Count,
            item.NumericCount > 0 ? FormatDecimal(item.Sum / item.NumericCount) : "-",
            item.Min is null ? "-" : FormatDecimal(item.Min.Value),
            item.Max is null ? "-" : FormatDecimal(item.Max.Value)))
        .ToList();

    List<SensorStat> sensorStats = sensorAccumulators.Values
        .OrderByDescending(item => item.Count)
        .Select(item => new SensorStat(
            item.SensorId,
            item.Count,
            item.LastTimestamp,
            string.Join(", ", item.Types.OrderBy(type => type))))
        .ToList();

    return new DashboardPayload(
        DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"),
        counters,
        measurements,
        analyses,
        videoStreams,
        typeStats,
        sensorStats);
}

async Task<DashboardActionResult> RunDashboardAnalysisAsync(string sensorId, string tipoDado, int count)
{
    sensorId = sensorId.Trim().ToUpperInvariant();
    tipoDado = NormalizeAnalysisType(tipoDado);

    if (sensorId.Length == 0)
    {
        return new DashboardActionResult(false, "Indica o sensor para gerar a analise.");
    }

    List<string> measurements = await GetLatestMeasurementsForAnalysisAsync(sensorId, tipoDado, count);
    if (measurements.Count == 0)
    {
        return new DashboardActionResult(false, $"Nao existem medicoes guardadas para {sensorId}/{tipoDado}.");
    }

    AnalysisRpcResult analysis = await RequestAnalysisAsync(BuildAnalysisScope(sensorId, tipoDado), measurements);
    if (!analysis.Success)
    {
        return new DashboardActionResult(false, $"Analise RPC falhou: {analysis.Error}");
    }

    await GravarAnaliseAsync(analysis, sensorId, tipoDado);
    return new DashboardActionResult(
        true,
        $"Analise guardada: sensor={sensorId}, tipo={tipoDado}, n={analysis.TotalRecords}, risco={analysis.RiskLevel}, media={analysis.AverageValue}.");
}

async Task<DashboardActionResult> ClearDashboardDataAsync()
{
    List<string> dataTypes = new();

    await databaseLock.WaitAsync();
    try
    {
        using SqliteConnection connection = new(DB_CONNECTION_STRING);
        connection.Open();

        using (SqliteCommand typesCommand = connection.CreateCommand())
        {
            typesCommand.CommandText = "SELECT DISTINCT TipoDado FROM Medicoes";
            using SqliteDataReader reader = typesCommand.ExecuteReader();
            while (reader.Read())
            {
                dataTypes.Add(reader.GetString(0));
            }
        }

        using (SqliteTransaction transaction = connection.BeginTransaction())
        {
            using SqliteCommand deleteMeasurements = connection.CreateCommand();
            deleteMeasurements.Transaction = transaction;
            deleteMeasurements.CommandText = "DELETE FROM Medicoes";
            deleteMeasurements.ExecuteNonQuery();

            using SqliteCommand deleteAnalyses = connection.CreateCommand();
            deleteAnalyses.Transaction = transaction;
            deleteAnalyses.CommandText = "DELETE FROM Analises";
            deleteAnalyses.ExecuteNonQuery();

            using SqliteCommand resetIds = connection.CreateCommand();
            resetIds.Transaction = transaction;
            resetIds.CommandText = "DELETE FROM sqlite_sequence WHERE name IN ('Medicoes', 'Analises')";
            resetIds.ExecuteNonQuery();

            transaction.Commit();
        }
    }
    finally
    {
        databaseLock.Release();
    }

    int deletedFiles = ClearDataFiles(dataTypes);
    Console.WriteLine($"[DASHBOARD] Dados limpos: base de dados e {deletedFiles} ficheiro(s).");
    return new DashboardActionResult(true, $"Dados limpos com sucesso. Ficheiros removidos/limpos: {deletedFiles}.");
}

int ClearDataFiles(IEnumerable<string> dataTypes)
{
    HashSet<string> files = new(StringComparer.OrdinalIgnoreCase);

    foreach (string dataType in dataTypes)
    {
        string normalizedType = dataType.Trim().ToUpperInvariant();
        if (normalizedType.Length > 0)
        {
            files.Add(Path.Combine(Environment.CurrentDirectory, $"{normalizedType}.txt"));
        }
    }

    foreach (string file in Directory.GetFiles(Environment.CurrentDirectory, "VIDEO_*.txt"))
    {
        files.Add(file);
    }

    int cleared = 0;
    foreach (string file in files)
    {
        if (!File.Exists(file))
        {
            continue;
        }

        string fileName = Path.GetFileName(file);
        Mutex fileMutex = fileMutexes.GetOrAdd(fileName, _ => new Mutex());

        fileMutex.WaitOne();
        try
        {
            File.WriteAllText(file, "");
            cleared++;
        }
        finally
        {
            fileMutex.ReleaseMutex();
        }
    }

    return cleared;
}

List<VideoStreamView> ReadVideoStreams()
{
    List<VideoStreamView> streams = new();

    foreach (string file in Directory.GetFiles(Environment.CurrentDirectory, "VIDEO_*.txt"))
    {
        string sensorId = Path.GetFileNameWithoutExtension(file).Replace("VIDEO_", "", StringComparison.OrdinalIgnoreCase);
        Mutex fileMutex = fileMutexes.GetOrAdd(Path.GetFileName(file), _ => new Mutex());

        fileMutex.WaitOne();
        try
        {
            string gatewayId = "-";
            string receivedAt = "-";
            int frameCount = 0;
            string startedAt = "-";
            string endedAt = "-";

            foreach (string line in File.ReadLines(file))
            {
                if (line.StartsWith("--- VIDEO BATCH", StringComparison.OrdinalIgnoreCase))
                {
                    AddVideoStreamIfComplete(streams, sensorId, gatewayId, receivedAt, startedAt, endedAt, frameCount, file);
                    gatewayId = ExtractVideoHeaderValue(line, "Gateway:");
                    receivedAt = ExtractVideoHeaderValue(line, "Recebido:");
                    frameCount = 0;
                    startedAt = "-";
                    endedAt = "-";
                    continue;
                }

                if (line.StartsWith("VIDEO_START|", StringComparison.OrdinalIgnoreCase))
                {
                    string[] parts = line.Split('|');
                    if (parts.Length >= 3)
                    {
                        startedAt = parts[2];
                    }
                    continue;
                }

                if (line.StartsWith("VIDEO_FRAME|", StringComparison.OrdinalIgnoreCase))
                {
                    frameCount++;
                    continue;
                }

                if (line.StartsWith("VIDEO_END|", StringComparison.OrdinalIgnoreCase))
                {
                    string[] parts = line.Split('|');
                    if (parts.Length >= 3)
                    {
                        endedAt = parts[2];
                    }
                }
            }

            AddVideoStreamIfComplete(streams, sensorId, gatewayId, receivedAt, startedAt, endedAt, frameCount, file);
        }
        catch
        {
        }
        finally
        {
            fileMutex.ReleaseMutex();
        }
    }

    return streams
        .OrderByDescending(stream => stream.ReceivedAt)
        .Take(20)
        .ToList();
}

void AddVideoStreamIfComplete(
    List<VideoStreamView> streams,
    string sensorId,
    string gatewayId,
    string receivedAt,
    string startedAt,
    string endedAt,
    int frameCount,
    string file)
{
    if (frameCount <= 0)
    {
        return;
    }

    streams.Add(new VideoStreamView(
        sensorId,
        gatewayId,
        receivedAt,
        startedAt,
        endedAt,
        frameCount,
        Path.GetFileName(file)));
}

string ExtractVideoHeaderValue(string line, string label)
{
    int start = line.IndexOf(label, StringComparison.OrdinalIgnoreCase);
    if (start < 0)
    {
        return "-";
    }

    start += label.Length;
    int end = line.IndexOf('|', start);
    if (end < 0)
    {
        end = line.IndexOf("---", start, StringComparison.OrdinalIgnoreCase);
    }

    if (end < 0)
    {
        end = line.Length;
    }

    return line[start..end].Trim();
}

long ReadLong(SqliteConnection connection, string sql)
{
    using SqliteCommand command = connection.CreateCommand();
    command.CommandText = sql;
    object? result = command.ExecuteScalar();
    return result == null || result == DBNull.Value ? 0 : Convert.ToInt64(result, CultureInfo.InvariantCulture);
}

string ReadText(SqliteConnection connection, string sql)
{
    using SqliteCommand command = connection.CreateCommand();
    command.CommandText = sql;
    object? result = command.ExecuteScalar();
    return result == null || result == DBNull.Value ? "-" : result.ToString() ?? "-";
}

bool TryParseDecimal(string value, out decimal result)
{
    string normalized = value.Trim().Replace(',', '.');
    return decimal.TryParse(normalized, NumberStyles.Number, CultureInfo.InvariantCulture, out result);
}

string FormatDecimal(decimal value) =>
    value.ToString("0.##", CultureInfo.InvariantCulture);

string GetDashboardHtml() =>
"""
<!doctype html>
<html lang="pt">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>OneHealth Dashboard</title>
  <style>
    :root {
  --ink: #0f172a;       /* Tom escuro azulado */
  --muted: #64748b;     /* Cinza azulado para texto secundário */
  --paper: #f8fafc;
  --card: rgba(255, 255, 255, 0.84);
  --line: rgba(15, 23, 42, 0.13);
  --green: #2563eb;     /* Substituído por Azul Principal */
  --mint: #93c5fd;      /* Substituído por Azul Claro */
  --amber: #e3a72f;
  --red: #b94a48;
  --blue: #1e3a8a;      /* Azul escuro secundário */
  --shadow: 0 24px 70px rgba(15, 23, 42, 0.16);
}

    * { box-sizing: border-box; }

    body {
  margin: 0;
  color: var(--ink);
  font-family: "Trebuchet MS", "Segoe UI", sans-serif;
  background:
    radial-gradient(circle at top left, rgba(147, 197, 253, .75), transparent 32rem),
    radial-gradient(circle at 88% 8%, rgba(227, 167, 47, .28), transparent 24rem),
    linear-gradient(135deg, #f0f4f8 0%, #e2e8f0 48%, #cbd5e1 100%);
  min-height: 100vh;
}

    .shell {
      width: min(1220px, calc(100% - 32px));
      margin: 0 auto;
      padding: 34px 0 44px;
    }

    header {
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 18px;
      align-items: end;
      margin-bottom: 24px;
    }

    .eyebrow {
      color: var(--green);
      font-size: 13px;
      font-weight: 800;
      letter-spacing: .16em;
      text-transform: uppercase;
    }

    h1 {
      margin: 6px 0 0;
      font-family: Georgia, "Times New Roman", serif;
      font-size: clamp(38px, 6vw, 72px);
      line-height: .92;
      letter-spacing: -.055em;
    }

    .status-pill {
      display: inline-flex;
      align-items: center;
      gap: 10px;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: rgba(255, 252, 245, .66);
      padding: 12px 16px;
      box-shadow: var(--shadow);
      color: var(--muted);
      white-space: nowrap;
    }

    .dot {
      width: 10px;
      height: 10px;
      border-radius: 999px;
      background: var(--green);
      box-shadow: 0 0 0 8px rgba(37, 99, 235, .12);
      animation: pulse 1.6s infinite;
    }

    .grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 14px;
      margin-bottom: 18px;
    }

    .card {
      border: 1px solid var(--line);
      border-radius: 26px;
      background: var(--card);
      box-shadow: var(--shadow);
      backdrop-filter: blur(18px);
      overflow: hidden;
    }

    .metric {
      padding: 20px;
      position: relative;
    }

    .metric::after {
      content: "";
      position: absolute;
      inset: auto -18px -28px auto;
      width: 92px;
      height: 92px;
      border-radius: 50%;
      background: rgba(37, 99, 235, .10);
    }

    .metric span {
      display: block;
      color: var(--muted);
      font-size: 13px;
      font-weight: 800;
      text-transform: uppercase;
      letter-spacing: .08em;
    }

    .metric strong {
      display: block;
      margin-top: 10px;
      font-size: 34px;
      letter-spacing: -.04em;
    }

    .layout {
      display: grid;
      grid-template-columns: 1.3fr .7fr;
      gap: 18px;
      align-items: start;
    }

    .panel {
      padding: 20px;
    }

    .panel h2 {
      margin: 0 0 14px;
      font-family: Georgia, "Times New Roman", serif;
      font-size: 28px;
      letter-spacing: -.03em;
    }

    .table-wrap {
      overflow-x: auto;
      border-radius: 18px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, .38);
    }

    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 720px;
    }

    th, td {
      padding: 12px 14px;
      text-align: left;
      border-bottom: 1px solid var(--line);
      font-size: 14px;
    }

    th {
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: .09em;
      background: rgba(255, 255, 255, .42);
    }

    tr:last-child td { border-bottom: 0; }

    .tag {
      display: inline-flex;
      border-radius: 999px;
      padding: 5px 9px;
      background: rgba(49, 95, 143, .11);
      color: var(--blue);
      font-weight: 800;
      font-size: 12px;
    }

    .risk {
      color: white;
      background: var(--green);
    }

    .risk.MEDIO { background: var(--amber); }
    .risk.ALTO { background: var(--red); }

    .side-stack {
      display: grid;
      gap: 18px;
    }

    .stat-list {
      display: grid;
      gap: 10px;
    }

    .stat-row {
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 10px;
      padding: 12px;
      border: 1px solid var(--line);
      border-radius: 16px;
      background: rgba(255, 255, 255, .36);
    }

    .stat-row b { display: block; }
    .stat-row small { color: var(--muted); }

    .analytics-grid {
      display: grid;
      grid-template-columns: minmax(0, 1.7fr) minmax(320px, .8fr);
      gap: 18px;
      margin-top: 18px;
    }

    .chart-wrap {
      min-height: 290px;
      border: 1px solid var(--line);
      border-radius: 18px;
      background:
        linear-gradient(180deg, rgba(255, 255, 255, .72), rgba(255, 255, 255, .28)),
        radial-gradient(circle at 12% 18%, rgba(37, 99, 235, .10), transparent 28%);
      overflow: hidden;
    }

    .chart-svg {
      display: block;
      width: 100%;
      height: 290px;
    }

    .chart-axis {
      stroke: rgba(30, 44, 37, .24);
      stroke-width: 1;
    }

    .chart-grid {
      stroke: rgba(30, 44, 37, .10);
      stroke-width: 1;
    }

    .chart-label {
      fill: var(--muted);
      font-size: 12px;
      font-weight: 700;
    }

    .chart-line {
      fill: none;
      stroke-width: 3;
      stroke-linecap: round;
      stroke-linejoin: round;
    }

    .chart-point {
      fill: white;
      stroke-width: 2.5;
    }

    .chart-point.outlier {
      fill: var(--red);
      stroke: white;
      stroke-width: 2;
    }

    .legend {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 10px;
      color: var(--muted);
      font-size: 13px;
    }

    .legend span {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 6px 9px;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: rgba(255, 255, 255, .46);
    }

    .legend i {
      width: 10px;
      height: 10px;
      border-radius: 999px;
      display: inline-block;
    }

    .insight-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
    }

    .insight-card {
      padding: 12px;
      border: 1px solid var(--line);
      border-radius: 16px;
      background: rgba(255, 255, 255, .38);
    }

    .insight-card span {
      display: block;
      color: var(--muted);
      font-size: 11px;
      font-weight: 800;
      letter-spacing: .08em;
      text-transform: uppercase;
    }

    .insight-card strong {
      display: block;
      margin-top: 6px;
      font-size: 18px;
    }

    .outlier-list {
      display: grid;
      gap: 10px;
    }

    .outlier-row {
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 10px;
      padding: 12px;
      border: 1px solid rgba(185, 74, 72, .22);
      border-radius: 16px;
      background: rgba(185, 74, 72, .07);
    }

    .outlier-row b { display: block; }
    .outlier-row small { color: var(--muted); }

    .empty {
      padding: 18px;
      color: var(--muted);
      border: 1px dashed var(--line);
      border-radius: 18px;
    }

    .toolbar {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
      margin: 0 0 14px;
    }

    .field {
      display: grid;
      gap: 6px;
    }

    .field label {
      color: var(--muted);
      font-size: 11px;
      font-weight: 800;
      letter-spacing: .08em;
      text-transform: uppercase;
    }

    input, select, button {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: rgba(255, 255, 255, .62);
      color: var(--ink);
      padding: 11px 12px;
      font: inherit;
    }

    button {
      cursor: pointer;
      border-color: rgba(37, 99, 235, .28);
      background: var(--green);
      color: white;
      font-weight: 800;
      transition: transform .16s ease, filter .16s ease;
    }

    button:hover { transform: translateY(-1px); filter: brightness(1.05); }

    button.danger {
      border-color: rgba(185, 74, 72, .34);
      background: var(--red);
    }

    .action-row {
      display: grid;
      grid-template-columns: 1fr 180px 140px 170px;
      gap: 10px;
      align-items: end;
      margin-bottom: 14px;
    }

    .notice {
      color: var(--muted);
      font-size: 13px;
      min-height: 20px;
      margin-bottom: 10px;
    }

    .danger-zone {
      margin-top: 14px;
      padding: 14px;
      border: 1px solid rgba(185, 74, 72, .24);
      border-radius: 18px;
      background: rgba(185, 74, 72, .07);
      display: grid;
      grid-template-columns: 1fr 180px;
      gap: 12px;
      align-items: center;
    }

    .danger-zone b { display: block; color: var(--red); }
    .danger-zone small { color: var(--muted); }

    @keyframes pulse {
      0%, 100% { transform: scale(1); opacity: 1; }
      50% { transform: scale(.72); opacity: .55; }
    }

    @media (max-width: 900px) {
      header, .layout, .analytics-grid { grid-template-columns: 1fr; }
      .grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .toolbar, .action-row, .danger-zone { grid-template-columns: 1fr; }
      .status-pill { justify-content: center; }
    }

    @media (max-width: 560px) {
      .grid { grid-template-columns: 1fr; }
      .shell { width: min(100% - 20px, 1220px); padding-top: 22px; }
    }
  </style>
</head>
<body>
  <main class="shell">
    <header>
      <div>
        <div class="eyebrow">Sistemas Distribuidos · OneHealth</div>
        <h1>Dashboard de monitorizacao</h1>
      </div>
      <div class="status-pill"><span class="dot"></span><span id="last-update">a iniciar...</span></div>
    </header>

    <section class="grid">
      <article class="card metric"><span>Medicoes</span><strong id="total-measurements">0</strong></article>
      <article class="card metric"><span>Sensores</span><strong id="total-sensors">0</strong></article>
      <article class="card metric"><span>Tipos de dados</span><strong id="total-types">0</strong></article>
      <article class="card metric"><span>Analises RPC</span><strong id="total-analyses">0</strong></article>
    </section>

    <section class="layout">
      <article class="card panel">
        <h2>Ultimas medicoes</h2>
        <div class="toolbar">
          <div class="field">
            <label for="filter-sensor">Sensor</label>
            <select id="filter-sensor"><option value="">Todos</option></select>
          </div>
          <div class="field">
            <label for="filter-type">Tipo</label>
            <select id="filter-type"><option value="">Todos</option></select>
          </div>
          <div class="field">
            <label for="filter-text">Pesquisa</label>
            <input id="filter-text" placeholder="gateway, valor, data...">
          </div>
          <div class="field">
            <label for="filter-limit">Limite</label>
            <select id="filter-limit">
              <option value="15">15 linhas</option>
              <option value="25">25 linhas</option>
              <option value="40">40 linhas</option>
            </select>
          </div>
        </div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr><th>ID</th><th>Gateway</th><th>Sensor</th><th>Timestamp</th><th>Tipo</th><th>Valor</th></tr>
            </thead>
            <tbody id="measurements"></tbody>
          </table>
        </div>
      </article>

      <aside class="side-stack">
        <article class="card panel">
          <h2>Estatisticas por tipo</h2>
          <div class="stat-list" id="type-stats"></div>
        </article>

        <article class="card panel">
          <h2>Sensores</h2>
          <div class="stat-list" id="sensor-stats"></div>
        </article>
      </aside>
    </section>

    <section class="analytics-grid">
      <article class="card panel">
        <h2>Historico das medicoes</h2>
        <div class="toolbar">
          <div class="field">
            <label for="chart-sensor">Sensor</label>
            <select id="chart-sensor"><option value="">Todos</option></select>
          </div>
          <div class="field">
            <label for="chart-type">Tipo</label>
            <select id="chart-type"><option value="">Todos</option></select>
          </div>
          <div class="field">
            <label for="chart-window">Janela</label>
            <select id="chart-window">
              <option value="40">40 pontos</option>
              <option value="80">80 pontos</option>
              <option value="120">120 pontos</option>
              <option value="200">200 pontos</option>
            </select>
          </div>
          <div class="field">
            <label>Outliers</label>
            <input id="chart-summary" readonly value="A calcular...">
          </div>
        </div>
        <div class="chart-wrap">
          <svg id="history-chart" class="chart-svg" viewBox="0 0 900 290" role="img" aria-label="Grafico historico das medicoes"></svg>
        </div>
        <div class="legend" id="chart-legend"></div>
      </article>

      <aside class="side-stack">
        <article class="card panel">
          <h2>Outliers detetados</h2>
          <div class="outlier-list" id="outliers-list"></div>
        </article>

        <article class="card panel">
          <h2>Leitura rapida</h2>
          <div class="insight-grid" id="insights"></div>
        </article>
      </aside>
    </section>

    <section class="card panel" style="margin-top:18px">
      <h2>Analises guardadas</h2>
      <div class="action-row">
        <div class="field">
          <label for="analysis-sensor">Sensor</label>
          <input id="analysis-sensor" placeholder="ex: S101">
        </div>
        <div class="field">
          <label for="analysis-type">Tipo de dado</label>
          <input id="analysis-type" placeholder="TEMP, HUM, RUIDO, PM2.5 ou vazio">
        </div>
        <div class="field">
          <label for="analysis-count">Registos</label>
          <input id="analysis-count" type="number" min="1" value="20">
        </div>
        <button id="analysis-button" type="button">Gerar analise</button>
      </div>
      <div class="notice" id="analysis-notice"></div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr><th>ID</th><th>Sensor</th><th>Data</th><th>N</th><th>Tipo de dado</th><th>Media</th><th>Max</th><th>Risco</th><th>Resumo</th></tr>
          </thead>
          <tbody id="analyses"></tbody>
        </table>
      </div>
      <div class="danger-zone">
        <div>
          <b>Limpar dados da demonstracao</b>
          <small>Apaga medicoes, analises e ficheiros de video/dados guardados no Servidor.</small>
        </div>
        <button class="danger" id="clear-button" type="button">Limpar dados</button>
      </div>
      <div class="notice" id="clear-notice"></div>
    </section>

    <section class="card panel" style="margin-top:18px">
      <h2>Streams de video recebidas</h2>
      <div class="table-wrap">
        <table>
          <thead>
            <tr><th>Sensor</th><th>Gateway</th><th>Recebido</th><th>Inicio</th><th>Fim</th><th>Frames</th><th>Ficheiro</th></tr>
          </thead>
          <tbody id="videos"></tbody>
        </table>
      </div>
    </section>
  </main>

  <script>
    const byId = (id) => document.getElementById(id);
    const safe = (value) => value ?? "-";
    const html = (value) => String(safe(value)).replace(/[&<>"']/g, char => ({
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      "\"": "&quot;",
      "'": "&#39;"
    }[char]));
    let dashboardData = null;

    async function refreshDashboard() {
      try {
        const response = await fetch("/api/dashboard", { cache: "no-store" });
        if (!response.ok) throw new Error("HTTP " + response.status);
        const data = await response.json();
        dashboardData = data;

        byId("last-update").textContent = "atualizado: " + data.generatedAt;
        byId("total-measurements").textContent = data.counters.totalMeasurements;
        byId("total-sensors").textContent = data.counters.totalSensors;
        byId("total-types").textContent = data.counters.totalTypes;
        byId("total-analyses").textContent = data.counters.totalAnalyses;

        populateFilters(data);
        renderFilteredMeasurements();
        renderTypeStats(data.typeStats);
        renderSensorStats(data.sensorStats);
        renderHistoryChart(data.measurements);
        renderOutliers(data.measurements);
        renderInsights(data);
        renderAnalyses(data.analyses);
        renderVideos(data.videoStreams);
      } catch (error) {
        byId("last-update").textContent = "sem ligacao ao servidor web";
      }
    }

    function populateFilters(data) {
      updateSelect("filter-sensor", data.sensorStats.map(item => item.sensorId));
      updateSelect("filter-type", data.typeStats.map(item => item.tipoDado));
      updateSelect("chart-sensor", data.sensorStats.map(item => item.sensorId));
      updateSelect("chart-type", data.typeStats.map(item => item.tipoDado));
    }

    function updateSelect(id, values) {
      const select = byId(id);
      const current = select.value;
      const unique = [...new Set(values.filter(Boolean))].sort();
      select.innerHTML = `<option value="">Todos</option>` + unique.map(value => `<option value="${html(value)}">${html(value)}</option>`).join("");
      select.value = unique.includes(current) ? current : "";
    }

    function renderFilteredMeasurements() {
      if (!dashboardData) return;

      const sensor = byId("filter-sensor").value.toLowerCase();
      const type = byId("filter-type").value.toLowerCase();
      const text = byId("filter-text").value.trim().toLowerCase();
      const limit = Number(byId("filter-limit").value || 15);

      const items = dashboardData.measurements
        .filter(item => !sensor || item.sensorId.toLowerCase() === sensor)
        .filter(item => !type || item.tipoDado.toLowerCase() === type)
        .filter(item => {
          if (!text) return true;
          return [item.id, item.gatewayId, item.sensorId, item.timestamp, item.tipoDado, item.valor]
            .join(" ")
            .toLowerCase()
            .includes(text);
        })
        .slice(0, limit);

      renderMeasurements(items);
    }

    function renderMeasurements(items) {
      byId("measurements").innerHTML = items.length
        ? items.map(item => `
          <tr>
            <td>#${item.id}</td>
            <td>${html(item.gatewayId)}</td>
            <td><span class="tag">${html(item.sensorId)}</span></td>
            <td>${html(item.timestamp)}</td>
            <td>${html(item.tipoDado)}</td>
            <td><b>${html(item.valor)}</b></td>
          </tr>`).join("")
        : `<tr><td colspan="6"><div class="empty">Ainda nao existem medicoes.</div></td></tr>`;
    }

    function renderTypeStats(items) {
      byId("type-stats").innerHTML = items.length
        ? items.map(item => `
          <div class="stat-row">
            <div><b>${html(item.tipoDado)}</b><small>${item.count} registos · media ${html(item.average)}</small></div>
            <div><small>min ${html(item.min)}<br>max ${html(item.max)}</small></div>
          </div>`).join("")
        : `<div class="empty">Sem estatisticas por enquanto.</div>`;
    }

    function renderSensorStats(items) {
      byId("sensor-stats").innerHTML = items.length
        ? items.map(item => `
          <div class="stat-row">
            <div><b>${html(item.sensorId)}</b><small>${html(item.types)}</small></div>
            <div><small>${item.count} registos<br>${html(item.lastTimestamp)}</small></div>
          </div>`).join("")
        : `<div class="empty">Nenhum sensor com dados guardados.</div>`;
    }

    function renderHistoryChart(measurements) {
      const svg = byId("history-chart");
      const sensor = byId("chart-sensor").value.toLowerCase();
      const type = byId("chart-type").value.toLowerCase();
      const limit = Number(byId("chart-window").value || 40);

      let points = getNumericMeasurements(measurements)
        .filter(point => !sensor || point.sensorId.toLowerCase() === sensor)
        .filter(point => !type || point.type.toLowerCase() === type)
        .sort((a, b) => a.id - b.id)
        .slice(-limit)
        .map((point, index) => ({ ...point, index }));

      const outlierMap = detectOutlierPoints(points);
      byId("chart-summary").value = `${points.length} pontos / ${outlierMap.size} outlier(s)`;

      if (points.length < 2) {
        svg.innerHTML = `<text x="450" y="145" text-anchor="middle" class="chart-label">Sem dados numericos suficientes para desenhar o grafico.</text>`;
        byId("chart-legend").innerHTML = "";
        return;
      }

      const width = 900;
      const height = 290;
      const pad = { left: 58, right: 28, top: 24, bottom: 44 };
      const innerWidth = width - pad.left - pad.right;
      const innerHeight = height - pad.top - pad.bottom;
      let min = Math.min(...points.map(point => point.value));
      let max = Math.max(...points.map(point => point.value));
      const spread = Math.max(max - min, 1);
      min -= spread * 0.12;
      max += spread * 0.12;

      const xFor = index => pad.left + (points.length === 1 ? 0 : index * innerWidth / (points.length - 1));
      const yFor = value => pad.top + (max - value) * innerHeight / (max - min || 1);
      const colors = ["#2563eb", "#1e3a8a", "#b94a48", "#e3a526", "#6f6aa8", "#2c8c99", "#8a6a2f"];
      const groups = new Map();

      for (const point of points) {
        const label = sensor ? point.type : `${point.sensorId}/${point.type}`;
        if (!groups.has(label)) groups.set(label, []);
        groups.get(label).push(point);
      }

      const grid = [0, .25, .5, .75, 1].map(step => {
        const y = pad.top + innerHeight * step;
        const value = max - (max - min) * step;
        return `<line class="chart-grid" x1="${pad.left}" y1="${y}" x2="${width - pad.right}" y2="${y}"></line>
                <text class="chart-label" x="${pad.left - 10}" y="${y + 4}" text-anchor="end">${html(formatNumber(value))}</text>`;
      }).join("");

      const lines = [...groups.entries()].map(([label, group], groupIndex) => {
        const color = colors[groupIndex % colors.length];
        const pathPoints = group.map(point => `${xFor(point.index)},${yFor(point.value)}`).join(" ");
        return `<polyline class="chart-line" stroke="${color}" points="${pathPoints}"></polyline>`;
      }).join("");

      const circles = points.map(point => {
        const groupIndex = [...groups.keys()].indexOf(sensor ? point.type : `${point.sensorId}/${point.type}`);
        const color = colors[groupIndex % colors.length];
        const outlier = outlierMap.has(point.id);
        const title = `${point.sensorId} ${point.type}: ${formatNumber(point.value)} (${formatTimestamp(point.timestamp)})`;
        return `<circle class="chart-point${outlier ? " outlier" : ""}" cx="${xFor(point.index)}" cy="${yFor(point.value)}" r="${outlier ? 5 : 3.6}" stroke="${color}">
                  <title>${html(title)}</title>
                </circle>`;
      }).join("");

      const first = points[0];
      const last = points[points.length - 1];
      svg.innerHTML = `
        ${grid}
        <line class="chart-axis" x1="${pad.left}" y1="${height - pad.bottom}" x2="${width - pad.right}" y2="${height - pad.bottom}"></line>
        <line class="chart-axis" x1="${pad.left}" y1="${pad.top}" x2="${pad.left}" y2="${height - pad.bottom}"></line>
        ${lines}
        ${circles}
        <text class="chart-label" x="${pad.left}" y="${height - 14}">${html(formatTimestamp(first.timestamp))}</text>
        <text class="chart-label" x="${width - pad.right}" y="${height - 14}" text-anchor="end">${html(formatTimestamp(last.timestamp))}</text>`;

      byId("chart-legend").innerHTML = [...groups.keys()].map((label, index) =>
        `<span><i style="background:${colors[index % colors.length]}"></i>${html(label)}</span>`
      ).join("");
    }

    function renderOutliers(measurements) {
      const points = getNumericMeasurements(measurements);
      const outliers = [...detectOutlierPoints(points).values()]
        .sort((a, b) => b.point.id - a.point.id)
        .slice(0, 6);

      byId("outliers-list").innerHTML = outliers.length
        ? outliers.map(outlier => `
          <div class="outlier-row">
            <div>
              <b>${html(outlier.point.sensorId)} / ${html(outlier.point.type)}</b>
              <small>${html(formatTimestamp(outlier.point.timestamp))} - esperado ${html(formatNumber(outlier.low))} a ${html(formatNumber(outlier.high))}</small>
            </div>
            <strong>${html(formatNumber(outlier.point.value))}</strong>
          </div>`).join("")
        : `<div class="empty">Sem outliers relevantes nos ultimos registos numericos.</div>`;
    }

    function renderInsights(data) {
      const numeric = getNumericMeasurements(data.measurements).sort((a, b) => a.id - b.id);
      const newest = numeric[numeric.length - 1];
      const activeSensor = data.sensorStats[0]?.sensorId ?? "-";
      const topType = data.typeStats[0]?.tipoDado ?? "-";
      const outlierCount = detectOutlierPoints(numeric).size;
      const trend = calculateTrend(numeric);

      byId("insights").innerHTML = [
        ["Ultimo valor", newest ? `${newest.sensorId}/${newest.type} ${formatNumber(newest.value)}` : "-"],
        ["Sensor mais ativo", activeSensor],
        ["Tipo dominante", topType],
        ["Outliers", String(outlierCount)],
        ["Tendencia", trend],
        ["Historico usado", `${numeric.length} registos`]
      ].map(([label, value]) => `
        <div class="insight-card">
          <span>${html(label)}</span>
          <strong>${html(value)}</strong>
        </div>`).join("");
    }

    function getNumericMeasurements(items) {
      return items
        .map(item => {
          const value = parseNumericValue(item.valor);
          if (value === null) return null;

          return {
            item,
            id: Number(item.id),
            sensorId: String(item.sensorId || ""),
            type: String(item.tipoDado || ""),
            timestamp: String(item.timestamp || ""),
            value
          };
        })
        .filter(Boolean);
    }

    function parseNumericValue(value) {
      const match = String(value ?? "").replace(",", ".").match(/-?\d+(\.\d+)?/);
      if (!match) return null;

      const parsed = Number(match[0]);
      return Number.isFinite(parsed) ? parsed : null;
    }

    function detectOutlierPoints(points) {
      const groups = new Map();
      for (const point of points) {
        const key = `${point.sensorId}|${point.type}`;
        if (!groups.has(key)) groups.set(key, []);
        groups.get(key).push(point);
      }

      const outliers = new Map();
      for (const group of groups.values()) {
        if (group.length < 4) continue;

        const values = group.map(point => point.value).sort((a, b) => a - b);
        const q1 = quantile(values, .25);
        const q3 = quantile(values, .75);
        const iqr = q3 - q1;
        const low = q1 - 1.5 * iqr;
        const high = q3 + 1.5 * iqr;

        for (const point of group) {
          if (point.value < low || point.value > high) {
            outliers.set(point.id, { point, low, high });
          }
        }
      }

      return outliers;
    }

    function quantile(values, q) {
      if (!values.length) return 0;

      const index = (values.length - 1) * q;
      const lower = Math.floor(index);
      const upper = Math.ceil(index);
      const weight = index - lower;
      return values[lower] * (1 - weight) + values[upper] * weight;
    }

    function calculateTrend(points) {
      if (!points.length) return "-";

      const newest = points[points.length - 1];
      const group = points.filter(point => point.sensorId === newest.sensorId && point.type === newest.type);
      if (group.length < 6) return "Poucos dados";

      const last = group.slice(-5);
      const previous = group.slice(-10, -5);
      if (previous.length === 0) return "Poucos dados";

      const lastAverage = average(last.map(point => point.value));
      const previousAverage = average(previous.map(point => point.value));
      const delta = lastAverage - previousAverage;
      if (Math.abs(delta) < 0.1) return "Estavel";

      return delta > 0 ? `A subir +${formatNumber(delta)}` : `A descer ${formatNumber(delta)}`;
    }

    function average(values) {
      return values.reduce((sum, value) => sum + value, 0) / Math.max(values.length, 1);
    }

    function formatNumber(value) {
      if (!Number.isFinite(Number(value))) return "-";
      return Number(value).toFixed(2).replace(/\.?0+$/, "");
    }

    function formatTimestamp(timestamp) {
      return String(timestamp || "-").replace("T", " ");
    }

    function renderVideos(items) {
      byId("videos").innerHTML = items.length
        ? items.map(item => `
          <tr>
            <td><span class="tag">${html(item.sensorId)}</span></td>
            <td>${html(item.gatewayId)}</td>
            <td>${html(item.receivedAt)}</td>
            <td>${html(item.startedAt)}</td>
            <td>${html(item.endedAt)}</td>
            <td><b>${item.frameCount}</b></td>
            <td>${html(item.fileName)}</td>
          </tr>`).join("")
        : `<tr><td colspan="7"><div class="empty">Ainda nao existem streams de video guardadas.</div></td></tr>`;
    }

    function renderAnalyses(items) {
      byId("analyses").innerHTML = items.length
        ? items.map(item => {
          const averageValue = stripSensorPrefix(item.averageValue, item.sensorId);
          const maxValue = stripSensorPrefix(item.maxValue, item.sensorId);
          const summary = stripSensorPrefix(item.summary, item.sensorId);

          return `
          <tr>
            <td>#${item.id}</td>
            <td><span class="tag">${html(item.sensorId)}</span></td>
            <td>${html(item.generatedAt)}</td>
            <td>${html(item.totalRecords)}</td>
            <td>${html(item.tipoDado)}</td>
            <td>${html(averageValue)}</td>
            <td>${html(maxValue)}</td>
            <td><span class="tag risk ${html(item.riskLevel)}">${html(item.riskLevel)}</span></td>
            <td>${html(summary)}</td>
          </tr>`;
        }).join("")
        : `<tr><td colspan="9"><div class="empty">Ainda nao existem analises.</div></td></tr>`;
    }

    function stripSensorPrefix(value, sensorId) {
      const sensor = String(sensorId || "").trim();
      if (!sensor || sensor === "-" || sensor.toUpperCase() === "LOTE") return value;

      return String(safe(value))
        .replaceAll(sensor + "/", "")
        .replaceAll(sensor.toUpperCase() + "/", "")
        .replaceAll(sensor.toLowerCase() + "/", "");
    }

    async function requestAnalysis() {
      const sensorId = byId("analysis-sensor").value.trim();
      const tipoDado = byId("analysis-type").value.trim();
      const count = byId("analysis-count").value || "20";
      const notice = byId("analysis-notice");
      const button = byId("analysis-button");

      button.disabled = true;
      notice.textContent = "A pedir analise RPC...";

      try {
        const response = await fetch(`/api/reanalyze?sensorId=${encodeURIComponent(sensorId)}&tipoDado=${encodeURIComponent(tipoDado)}&count=${encodeURIComponent(count)}`, {
          method: "POST",
          cache: "no-store"
        });
        const result = await response.json();
        notice.textContent = result.message;
        await refreshDashboard();
      } catch (error) {
        notice.textContent = "Nao foi possivel pedir a analise.";
      } finally {
        button.disabled = false;
      }
    }

    async function clearAllData() {
      const typed = prompt("Esta acao apaga medicoes, analises e ficheiros de dados/video. Escreve LIMPAR para confirmar:");
      const notice = byId("clear-notice");
      const button = byId("clear-button");

      if (typed !== "LIMPAR") {
        notice.textContent = "Limpeza cancelada.";
        return;
      }

      button.disabled = true;
      notice.textContent = "A limpar dados...";

      try {
        const response = await fetch("/api/clear-data?confirm=LIMPAR", {
          method: "POST",
          cache: "no-store"
        });
        const result = await response.json();
        notice.textContent = result.message;
        await refreshDashboard();
      } catch (error) {
        notice.textContent = "Nao foi possivel limpar os dados.";
      } finally {
        button.disabled = false;
      }
    }

    ["filter-sensor", "filter-type", "filter-text", "filter-limit"].forEach(id => {
      byId(id).addEventListener("input", renderFilteredMeasurements);
      byId(id).addEventListener("change", renderFilteredMeasurements);
    });

    ["chart-sensor", "chart-type", "chart-window"].forEach(id => {
      byId(id).addEventListener("change", () => {
        if (dashboardData) renderHistoryChart(dashboardData.measurements);
      });
    });

    byId("analysis-button").addEventListener("click", requestAnalysis);
    byId("clear-button").addEventListener("click", clearAllData);

    refreshDashboard();
    setInterval(refreshDashboard, 2500);
  </script>
</body>
</html>
""";

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

    using SqliteCommand dedupIndexCommand = connection.CreateCommand();
    dedupIndexCommand.CommandText = @"
        CREATE INDEX IF NOT EXISTS IX_Medicoes_Dedup
        ON Medicoes (SensorId, Timestamp, TipoDado)";
    dedupIndexCommand.ExecuteNonQuery();

    using SqliteCommand analysisCommand = connection.CreateCommand();
    analysisCommand.CommandText = @"
        CREATE TABLE IF NOT EXISTS Analises (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            GatewayId TEXT NOT NULL,
            SensorId TEXT NOT NULL DEFAULT '-',
            TipoDado TEXT NOT NULL DEFAULT 'TODOS',
            GeneratedAt TEXT NOT NULL,
            TotalRecords TEXT NOT NULL,
            AverageValue TEXT NOT NULL,
            MaxValue TEXT NOT NULL,
            RiskLevel TEXT NOT NULL,
            Summary TEXT NOT NULL
        )";
    analysisCommand.ExecuteNonQuery();

    EnsureColumnExists(connection, "Analises", "SensorId", "TEXT NOT NULL DEFAULT '-'");
    EnsureColumnExists(connection, "Analises", "TipoDado", "TEXT NOT NULL DEFAULT 'TODOS'");

    Console.WriteLine("[SERVIDOR] Base de dados pronta.");
}

void EnsureColumnExists(SqliteConnection connection, string tableName, string columnName, string columnDefinition)
{
    bool exists = false;
    using (SqliteCommand checkCommand = connection.CreateCommand())
    {
        checkCommand.CommandText = $"PRAGMA table_info({tableName})";

        using SqliteDataReader reader = checkCommand.ExecuteReader();
        while (reader.Read())
        {
            if (reader.GetString(1).Equals(columnName, StringComparison.OrdinalIgnoreCase))
            {
                exists = true;
                break;
            }
        }
    }

    if (exists)
    {
        return;
    }

    using SqliteCommand alterCommand = connection.CreateCommand();
    alterCommand.CommandText = $"ALTER TABLE {tableName} ADD COLUMN {columnName} {columnDefinition}";
    alterCommand.ExecuteNonQuery();
}

record AnalysisRpcResult(
    bool Success,
    string GatewayId,
    string GeneratedAt,
    string TotalRecords,
    string AverageValue,
    string MaxValue,
    string RiskLevel,
    string Summary,
    string Error)
{
    public static AnalysisRpcResult Ok(
        string gatewayId,
        string generatedAt,
        string totalRecords,
        string averageValue,
        string maxValue,
        string riskLevel,
        string summary) =>
        new(true, gatewayId, generatedAt, totalRecords, averageValue, maxValue, riskLevel, summary, "");

    public static AnalysisRpcResult Fail(string error) =>
        new(false, "", "", "", "", "", "", "", error);
}

record MeasurementRow(string SensorId, string Timestamp, string TipoDado, string Valor);

record StructuredDataBatch(
    string GatewayId,
    string BatchId,
    string SentAt,
    int TotalRecords,
    List<StructuredMeasurement> Measurements,
    Dictionary<string, StructuredSummary> SummaryByType);

record StructuredMeasurement(string SensorId, string Timestamp, string Type, string Value);

record StructuredSummary(int Count, string Min, string Max, string Avg);

record DashboardPayload(
    string GeneratedAt,
    DashboardCounters Counters,
    List<MeasurementView> Measurements,
    List<AnalysisView> Analyses,
    List<VideoStreamView> VideoStreams,
    List<TypeStat> TypeStats,
    List<SensorStat> SensorStats);

record DashboardCounters(
    long TotalMeasurements,
    long TotalSensors,
    long TotalTypes,
    long TotalAnalyses,
    string LastTimestamp);

record MeasurementView(
    int Id,
    string GatewayId,
    string SensorId,
    string Timestamp,
    string TipoDado,
    string Valor);

record AnalysisView(
    int Id,
    string SensorId,
    string GeneratedAt,
    string TotalRecords,
    string TipoDado,
    string AverageValue,
    string MaxValue,
    string RiskLevel,
    string Summary);

record VideoStreamView(
    string SensorId,
    string GatewayId,
    string ReceivedAt,
    string StartedAt,
    string EndedAt,
    int FrameCount,
    string FileName);

record TypeStat(string TipoDado, int Count, string Average, string Min, string Max);

record SensorStat(string SensorId, int Count, string LastTimestamp, string Types);

record DashboardActionResult(bool Success, string Message);

class TypeAccumulator(string type)
{
    public string Type { get; } = type;
    public int Count { get; set; }
    public int NumericCount { get; set; }
    public decimal Sum { get; set; }
    public decimal? Min { get; set; }
    public decimal? Max { get; set; }
}

class SensorAccumulator(string sensorId)
{
    public string SensorId { get; } = sensorId;
    public int Count { get; set; }
    public string LastTimestamp { get; set; } = "-";
    public HashSet<string> Types { get; } = new(StringComparer.OrdinalIgnoreCase);
}
