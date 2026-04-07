using System;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace SimuladorGateway
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("A iniciar o Simulador de Gateway...");
            await Task.Delay(2000); // Espera 2 segundos para dar tempo de ligares o Servidor

            try
            {
                // Liga-se à porta 6000 do teu Servidor
                using TcpClient client = new TcpClient("127.0.0.1", 6000);
                using NetworkStream stream = client.GetStream();
                using StreamReader reader = new StreamReader(stream, Encoding.UTF8);
                using StreamWriter writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true };

                Console.WriteLine("Ligado ao Servidor! A enviar dados...\n");

                // Envia uma medição simples
                await writer.WriteLineAsync("DATA|S101|2026-04-07T15:00:00|TEMP|24.5");
                string? resposta = await reader.ReadLineAsync();
                Console.WriteLine($"[RESPOSTA DO SERVIDOR]: {resposta}");

                // Envia um Lote inteiro
                string lote = "DATA_BATCH|GW_CENTRO|2|S103;2026-04-07T15:02:00;PM2.5;30 / S101;2026-04-07T15:02:30;TEMP;25.0";
                await writer.WriteLineAsync(lote);
                resposta = await reader.ReadLineAsync();
                Console.WriteLine($"[RESPOSTA DO SERVIDOR]: {resposta}");

                Console.WriteLine("\nTestes concluídos! Podes fechar esta janela.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao ligar: {ex.Message}");
                Console.WriteLine("Lembraste-te de colocar o projeto 'Servidor' a correr primeiro?");
            }

            Console.ReadLine();
        }
    }
}