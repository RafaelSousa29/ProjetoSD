using System;
using System.IO;
using System.Collections.Generic;
using System.Threading;
using System.Linq;

const string MEDICOES_FILE = "medicoes.txt";
const int INTERVALO_GERACAO_MS = 1000; // Gera um novo registo a cada segundo

// Definições de dados simulados
var sensoresConfig = new Dictionary<string, (string tipo, double min, double max, string unidade)>
{
    { "TEMP", ("TEMP", 15.0, 35.0, "°C") },
    { "RUIDO", ("RUIDO", 40.0, 85.0, "dB") },
    { "HUM", ("HUM", 30.0, 90.0, "%") },
    { "LUZ", ("LUZ", 0.0, 100000.0, "lux") },
    { "CO2", ("CO2", 300.0, 1200.0, "ppm") }
};

Random random = new Random();
List<string> tiposDisponíveis = sensoresConfig.Keys.ToList();
object lockFicheiro = new object();
bool geradorAtivo = true;

Console.WriteLine($"[GERADOR] Iniciando gerador de dados aleatórios para '{MEDICOES_FILE}'");
Console.WriteLine($"[GERADOR] Intervalo de geração: {INTERVALO_GERACAO_MS}ms");
Console.WriteLine("[GERADOR] Tipos disponíveis: " + string.Join(", ", tiposDisponíveis));
Console.WriteLine("[GERADOR] Pressione CTRL+C para parar.\n");

// Thread para gerar dados
Thread geracaoThread = new Thread(GerarDados)
{
    Name = "GeradorDados",
    IsBackground = false
};
geracaoThread.Start();

// Esperar pelo término
geracaoThread.Join();

Console.WriteLine("\n[GERADOR] Programa encerrado.");
return;

void GerarDados()
{
    while (geradorAtivo)
    {
        try
        {
            // Selecionar um tipo aleatório
            string tipoSelecionado = tiposDisponíveis[random.Next(tiposDisponíveis.Count)];
            var (tipo, min, max, unidade) = sensoresConfig[tipoSelecionado];

            // Gerar valor aleatório com variação suave (não muito brusco)
            double valor = Math.Round(min + (random.NextDouble() * (max - min)), 2);

            // Formatar a linha
            string linha = $"{tipo}|{valor}";

            // Adicionar ao ficheiro (append) com lock
            lock (lockFicheiro)
            {
                try
                {
                    File.AppendAllText(MEDICOES_FILE, linha + Environment.NewLine);
                    Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] {tipoSelecionado}: {valor} {unidade}");
                }
                catch (IOException ex)
                {
                    Console.WriteLine($"[GERADOR] Aviso ao escrever: {ex.Message}");
                }
            }

            Thread.Sleep(INTERVALO_GERACAO_MS);
        }
        catch (ThreadInterruptedException)
        {
            Console.WriteLine("[GERADOR] Thread interrompida.");
            break;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[GERADOR] Erro: {ex.Message}");
            Thread.Sleep(1000);
        }
    }
}