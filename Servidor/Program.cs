using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

const int PORT = 6000;
const string DB_CONNECTION_STRING = "Data Source=OneHealth.db";

ConcurrentDictionary<string, Mutex> fileMutexes = new ConcurrentDictionary<string, Mutex>();

InitializeDatabase();

TcpListener listener = new TcpListener(IPAddress.Any, PORT);
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

                Console.WriteLine($"[SERVIDOR] Recebido: {line}");

                if (line.StartsWith("DATA_BATCH"))
                {
                    string[] mainParts = line.Split('|', 4);
                    if (mainParts.Length >= 4)
                    {
                        string gatewayId = mainParts[1].Trim();
                        string[] medicoes = mainParts[3].Split('#');

                        foreach (var medicao in medicoes)
                        {
                            if (string.IsNullOrWhiteSpace(medicao)) continue;

                            string[] p = medicao.Split('|');
                            if (p.Length >= 5 && p[0] == "DATA")
                            {
                                GravarDados(gatewayId, p[1], p[2], p[3], p[4]);
                            }
                        }
                    }
                    await writer.WriteLineAsync("LOTE_ACK|SUCESSO");
                }
                else
                {
                    await writer.WriteLineAsync("SERVER_ACK|SUCESSO");
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[SERVIDOR] Erro: {ex.Message}");
        }
    }
    Console.WriteLine("[SERVIDOR] Gateway desligado.");
}

void GravarDados(string gatewayId, string sensorId, string timestamp, string tipoDado, string valor)
{
    tipoDado = tipoDado.ToUpper();

    string fileName = $"{tipoDado}.txt";
    Mutex fileMutex = fileMutexes.GetOrAdd(fileName, new Mutex());

    fileMutex.WaitOne();
    try
    {
        string logEntry = $"{timestamp} | Gateway: {gatewayId} | Sensor: {sensorId} | Valor: {valor}";
        File.AppendAllText(fileName, logEntry + Environment.NewLine);
        Console.WriteLine($"[DEBUG] Ficheiro {fileName} atualizado em: {Path.GetFullPath(fileName)}");
    }
    finally
    {
        fileMutex.ReleaseMutex();
    }

    try
    {
        using (var connection = new SqliteConnection(DB_CONNECTION_STRING))
        {
            connection.Open();
            var command = connection.CreateCommand();
            command.CommandText = @"
                INSERT INTO Medicoes (GatewayId, SensorId, TipoDado, Valor, Timestamp)
                VALUES ($gatewayId, $sensorId, $tipoDado, $valor, $timestamp)";

            command.Parameters.AddWithValue("$gatewayId", gatewayId);
            command.Parameters.AddWithValue("$sensorId", sensorId);
            command.Parameters.AddWithValue("$tipoDado", tipoDado);
            command.Parameters.AddWithValue("$valor", valor);
            command.Parameters.AddWithValue("$timestamp", timestamp);

            command.ExecuteNonQuery();
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[SERVIDOR] Erro na BD: {ex.Message}");
    }
}

void InitializeDatabase()
{
    using (var connection = new SqliteConnection(DB_CONNECTION_STRING))
    {
        connection.Open();
        var command = connection.CreateCommand();
        command.CommandText = @"
            CREATE TABLE IF NOT EXISTS Medicoes (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                GatewayId TEXT,
                SensorId TEXT,
                TipoDado TEXT,
                Valor TEXT,
                Timestamp TEXT
            )";
        command.ExecuteNonQuery();
    }
    Console.WriteLine("[SERVIDOR] Base de Dados Pronta e Mutexes Ativos.");
}