using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

namespace Servidor
{
    class Program
    {
        const int PORT = 6000;
        const string DB_CONNECTION_STRING = "Data Source=OneHealth.db";

        // Dicionário seguro para gerir um Mutex independente para CADA ficheiro (Tarefa 3.1 e Fase 4)
        static ConcurrentDictionary<string, Mutex> fileMutexes = new ConcurrentDictionary<string, Mutex>();

        static async Task Main(string[] args)
        {
            // Inicializar a Base de Dados SQLite (Tarefa 3.2)
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
        }

        static void InitializeDatabase()
        {
            using (var connection = new SqliteConnection(DB_CONNECTION_STRING))
            {
                connection.Open();
                var command = connection.CreateCommand();
                // Criação da tabela para guardar os dados epidemiológicos
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
            Console.WriteLine("[SERVIDOR] Base de Dados Relacional (SQLite) pronta.");
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

                        Console.WriteLine($"[SERVIDOR] Recebido: {line}");

                        // Lógica de Processamento da Mensagem
                        if (line.StartsWith("DATA_BATCH"))
                        {
                            ProcessarLote(line);
                            // Tarefa 3.3: Enviar confirmação de lote
                            await writer.WriteLineAsync("BATCH_ACK|SUCESSO");
                        }
                        else if (line.StartsWith("DATA"))
                        {
                            ProcessarDadoIndividual(line);
                            await writer.WriteLineAsync("DATA_ACK|SUCESSO");
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[SERVIDOR] Erro na ligação: {ex.Message}");
                }
            }
            Console.WriteLine("[SERVIDOR] Gateway desligado.");
        }

        static void ProcessarLote(string lote)
        {
            // Estrutura combinada com os teus colegas: 
            // DATA_BATCH | GATEWAY_ID | N_REGISTOS | S101;2026-03-10T09:15;TEMP;22.5 / S102;2026...;RUIDO;70
            string[] parts = lote.Split('|');
            if (parts.Length >= 4)
            {
                string gatewayId = parts[1].Trim();
                string[] medicoes = parts[3].Split('/'); // O Gateway separa cada medição por '/' no lote

                foreach (var medicao in medicoes)
                {
                    if (string.IsNullOrWhiteSpace(medicao)) continue;

                    string[] dados = medicao.Split(';');
                    if (dados.Length == 4)
                    {
                        // dados: 0=SensorId, 1=Timestamp, 2=TipoDado, 3=Valor
                        GravarDados(gatewayId, dados[0], dados[1], dados[2], dados[3]);
                    }
                }
            }
        }

        static void ProcessarDadoIndividual(string linha)
        {
            // Estrutura: DATA | SENSOR_ID | TIMESTAMP | TIPO_DADO | VALOR
            string[] parts = linha.Split('|');
            if (parts.Length == 5)
            {
                GravarDados("GATEWAY_DIRETO", parts[1].Trim(), parts[2].Trim(), parts[3].Trim(), parts[4].Trim());
            }
        }

        static void GravarDados(string gatewayId, string sensorId, string timestamp, string tipoDado, string valor)
        {
            tipoDado = tipoDado.ToUpper(); // Normalizar para evitar Temp.txt e TEMP.txt

            // Tarefa 3.1: Gravação em ficheiros distintos organizados pelo tipo de medição
            string fileName = $"{tipoDado}.txt";

            // Vai buscar o Mutex deste ficheiro ou cria um novo se não existir
            Mutex fileMutex = fileMutexes.GetOrAdd(fileName, new Mutex());

            fileMutex.WaitOne(); // Bloqueia a thread para acesso sequencial ao ficheiro 
            try
            {
                string logEntry = $"{timestamp} | Gateway: {gatewayId} | Sensor: {sensorId} | Valor: {valor}";
                File.AppendAllText(fileName, logEntry + Environment.NewLine);
            }
            finally
            {
                fileMutex.ReleaseMutex(); // Liberta o ficheiro para outras threads
            }

            // Tarefa 3.2: Funcionalidade Extra - Guardar na BD Relacional SQLite
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
                Console.WriteLine($"[SERVIDOR] Erro ao gravar na BD: {ex.Message}");
            }
        }
    }
}