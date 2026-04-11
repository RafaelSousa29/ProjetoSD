using System;
using System.IO;
using System.Collections.Generic;
using System.Threading;
using System.Linq;

const string CSV_FILE = "../Gateway/sensors.csv";
const string DADOS_DIR = "../dados";
const string CONFIG_DIR = "../dados/config";
const string INTERVALO_GERACAO_MS = "1000";
const int TEMPO_VARIACAO_MIN = 30000;

var dadosReaisPorZona = new Dictionary<string, Dictionary<string, (double min, double max, double variacao)>>
{
    {
        "ZONA_PARQUE", new Dictionary<string, (double, double, double)>
        {
            { "TEMP", (10.0, 32.0, 0.5) },
            { "HUM", (30.0, 85.0, 2.0) },
            { "RUIDO", (40.0, 70.0, 1.5) },
            { "PM2.5", (5.0, 30.0, 1.0) }
        }
    },
    {
        "ZONA_ESCOLAR", new Dictionary<string, (double, double, double)>
        {
            { "TEMP", (18.0, 28.0, 0.3) },
            { "HUM", (40.0, 70.0, 1.0) },
            { "RUIDO", (55.0, 85.0, 3.0) },
            { "PM2.5", (15.0, 50.0, 2.0) }
        }
    },
    {
        "ZONA_CENTRO", new Dictionary<string, (double, double, double)>
        {
            { "TEMP", (15.0, 30.0, 0.8) },
            { "HUM", (35.0, 75.0, 1.5) },
            { "RUIDO", (65.0, 90.0, 2.0) },
            { "PM2.5", (20.0, 80.0, 3.0) }
        }
    },
    {
        "ZONA_PASSEIO", new Dictionary<string, (double, double, double)>
        {
            { "TEMP", (12.0, 30.0, 0.6) },
            { "HUM", (30.0, 80.0, 2.0) },
            { "RUIDO", (50.0, 75.0, 1.8) },
            { "PM2.5", (10.0, 40.0, 1.5) }
        }
    }
};

var valoresActuais = new Dictionary<string, Dictionary<string, double>>();
var tiposActuaisPerSensor = new Dictionary<string, string>(); // Track current config

object lockFicheiro = new object();
object lockConfig = new object();
bool geradorAtivo = true;

if (!Directory.Exists(DADOS_DIR))
{
    Directory.CreateDirectory(DADOS_DIR);
    Console.WriteLine($"[GERADOR] ✓ Pasta '{DADOS_DIR}' criada.");
}

if (!Directory.Exists(CONFIG_DIR))
{
    Directory.CreateDirectory(CONFIG_DIR);
    Console.WriteLine($"[GERADOR] ✓ Pasta '{CONFIG_DIR}' criada.");
}

Console.WriteLine("[GERADOR] Iniciando gerador de dados realistas por sensor");
Console.WriteLine($"[GERADOR] CSV: {CSV_FILE}");
Console.WriteLine($"[GERADOR] Pasta de dados: {DADOS_DIR}");
Console.WriteLine($"[GERADOR] Pasta de config: {CONFIG_DIR}");
Console.WriteLine("[GERADOR] Pressione CTRL+C para parar.\n");

var sensores = CarregarSensoresDoCSV();

if (sensores.Count == 0)
{
    Console.WriteLine("[GERADOR] ✗ Nenhum sensor encontrado no CSV!");
    return;
}

Console.WriteLine($"[GERADOR] ✓ {sensores.Count} sensores carregados:\n");
foreach (var sensor in sensores)
{
    Console.WriteLine($"  {sensor.SensorId} ({sensor.Zona}) - Tipos disponíveis: {string.Join(", ", sensor.Tipos)}");
    
    valoresActuais[sensor.SensorId] = new Dictionary<string, double>();
    tiposActuaisPerSensor[sensor.SensorId] = "";
    
    foreach (var tipo in sensor.Tipos)
    {
        if (dadosReaisPorZona[sensor.Zona].ContainsKey(tipo))
        {
            var (min, max, _) = dadosReaisPorZona[sensor.Zona][tipo];
            valoresActuais[sensor.SensorId][tipo] = min + (new Random().NextDouble() * (max - min));
        }
    }
}

Console.WriteLine();

List<Thread> threadsGeradores = new();

foreach (var sensor in sensores)
{
    Thread t = new Thread(() => GerarDadosPorSensor(sensor))
    {
        Name = $"Gerador-{sensor.SensorId}",
        IsBackground = false
    };
    t.Start();
    threadsGeradores.Add(t);
}

foreach (var t in threadsGeradores)
{
    t.Join();
}

Console.WriteLine("\n[GERADOR] Programa encerrado.");
return;

void GerarDadosPorSensor(SensorInfo sensor)
{
    string ficheiro = Path.Combine(DADOS_DIR, $"medicoes_{sensor.SensorId}.txt");
    string configFile = Path.Combine(CONFIG_DIR, $"config_{sensor.SensorId}.txt");
    Random random = new Random();
    DateTime ultimaVariacao = DateTime.Now;
    int tentativasConfig = 0;
    string ultimaConfig = "";

    while (geradorAtivo)
    {
        try
        {
            // Obter tipos activos
            string configActual = ObterConfigActiva(configFile);

            // Se a config mudou, limpar o ficheiro de medições
            if (!string.IsNullOrEmpty(configActual) && configActual != ultimaConfig)
            {
                ultimaConfig = configActual;
                tentativasConfig = 0;
                
                lock (lockFicheiro)
                {
                    try
                    {
                        if (File.Exists(ficheiro))
                        {
                            File.Delete(ficheiro);
                            Console.WriteLine($"[GERADOR] 🔄 {sensor.SensorId}: Configuração mudou - ficheiro reiniciado");
                        }
                    }
                    catch { }
                }
            }

            List<string> tiposActivos = string.IsNullOrEmpty(configActual) 
                ? new List<string>() 
                : configActual.Split(',').Select(t => t.Trim()).ToList();

            if (tiposActivos.Count == 0)
            {
                tentativasConfig++;
                if (tentativasConfig == 1)
                {
                    Console.WriteLine($"[GERADOR] ⏳ {sensor.SensorId}: Aguardando configuração do sensor...");
                }
                if (tentativasConfig > 60) // 60 segundos de espera
                {
                    Console.WriteLine($"[GERADOR] ⚠️  {sensor.SensorId}: Timeout - usando tipos padrão");
                    tiposActivos = sensor.Tipos;
                    ultimaConfig = string.Join(",", sensor.Tipos);
                }
                else
                {
                    Thread.Sleep(1000);
                    continue;
                }
            }
            else if (tentativasConfig > 0)
            {
                Console.WriteLine($"[GERADOR] ✓ {sensor.SensorId}: Gerando dados para - {string.Join(", ", tiposActivos)}");
                tentativasConfig = 0;
            }

            // Selecionar tipo aleatório
            string tipoSelecionado = tiposActivos[random.Next(tiposActivos.Count)];

            if (dadosReaisPorZona[sensor.Zona].ContainsKey(tipoSelecionado))
            {
                var (min, max, variacao) = dadosReaisPorZona[sensor.Zona][tipoSelecionado];

                // Inicializar valor se não existe
                if (!valoresActuais[sensor.SensorId].ContainsKey(tipoSelecionado))
                {
                    valoresActuais[sensor.SensorId][tipoSelecionado] = 
                        min + (random.NextDouble() * (max - min));
                }

                // Variação suave
                if ((DateTime.Now - ultimaVariacao).TotalMilliseconds > TEMPO_VARIACAO_MIN)
                {
                    valoresActuais[sensor.SensorId][tipoSelecionado] = 
                        min + (random.NextDouble() * (max - min));
                    ultimaVariacao = DateTime.Now;
                }
                else
                {
                    double valorActual = valoresActuais[sensor.SensorId][tipoSelecionado];
                    double delta = (random.NextDouble() - 0.5) * 2 * variacao;
                    valoresActuais[sensor.SensorId][tipoSelecionado] = 
                        Math.Max(min, Math.Min(max, valorActual + delta));
                }

                double valor = Math.Round(valoresActuais[sensor.SensorId][tipoSelecionado], 2);
                string linha = $"{tipoSelecionado}|{valor}";

                lock (lockFicheiro)
                {
                    try
                    {
                        File.AppendAllText(ficheiro, linha + Environment.NewLine);
                        Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] {sensor.SensorId} - {tipoSelecionado}: {valor}");
                    }
                    catch (IOException ex)
                    {
                        Console.WriteLine($"[GERADOR] ⚠️  {sensor.SensorId}: Erro ao escrever - {ex.Message}");
                    }
                }
            }

            Thread.Sleep(int.Parse(INTERVALO_GERACAO_MS));
        }
        catch (ThreadInterruptedException)
        {
            Console.WriteLine($"[GERADOR] Thread {sensor.SensorId} interrompida.");
            break;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[GERADOR] Erro em {sensor.SensorId}: {ex.Message}");
            Thread.Sleep(1000);
        }
    }
}

string ObterConfigActiva(string configFile)
{
    lock (lockConfig)
    {
        try
        {
            if (File.Exists(configFile))
            {
                var conteudo = File.ReadAllText(configFile).Trim();
                if (!string.IsNullOrWhiteSpace(conteudo))
                {
                    return conteudo;
                }
            }
        }
        catch { }
    }

    return "";
}

List<SensorInfo> CarregarSensoresDoCSV()
{
    var sensores = new List<SensorInfo>();

    try
    {
        if (!File.Exists(CSV_FILE))
        {
            Console.WriteLine($"[GERADOR] ✗ Ficheiro CSV não encontrado: {CSV_FILE}");
            return sensores;
        }

        var linhas = File.ReadAllLines(CSV_FILE).Skip(1);

        foreach (var linha in linhas)
        {
            if (string.IsNullOrWhiteSpace(linha))
                continue;

            var partes = linha.Split(';');
            if (partes.Length >= 4)
            {
                string sensorId = partes[0].Trim();
                string estado = partes[1].Trim();
                string zona = partes[2].Trim();
                var tipos = partes[3].Split(',').Select(t => t.Trim()).ToList();

                if (estado.ToLower() == "ativo" && dadosReaisPorZona.ContainsKey(zona))
                {
                    sensores.Add(new SensorInfo
                    {
                        SensorId = sensorId,
                        Estado = estado,
                        Zona = zona,
                        Tipos = tipos
                    });
                }
            }
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[GERADOR] Erro ao ler CSV: {ex.Message}");
    }

    return sensores;
}

class SensorInfo
{
    public string SensorId { get; set; } = "";
    public string Estado { get; set; } = "";
    public string Zona { get; set; } = "";
    public List<string> Tipos { get; set; } = new();
}