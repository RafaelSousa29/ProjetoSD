using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;

const int PORT = 7100;
const int MAX_PENDING_MESSAGES = 1000;

ConcurrentDictionary<string, Subscriber> subscribers = new(StringComparer.OrdinalIgnoreCase);
List<BrokerMessage> pendingMessages = new();
object pendingMessagesLock = new();
long nextMessageId = 0;

TcpListener listener = new(IPAddress.Any, PORT);
listener.Start();

Console.WriteLine($"[PUBSUB] Broker ativo na porta {PORT}.");
Console.WriteLine("[PUBSUB] SUBSCRIBE|id|topico e PUBLISH|topico|payload");

while (true)
{
    TcpClient client = await listener.AcceptTcpClientAsync();
    _ = Task.Run(() => HandleClientAsync(client));
}

async Task HandleClientAsync(TcpClient client)
{
    using (client)
    using (NetworkStream stream = client.GetStream())
    using (StreamReader reader = new(stream, Encoding.UTF8))
    using (StreamWriter writer = new(stream, Encoding.UTF8) { AutoFlush = true })
    {
        string? firstLine = await reader.ReadLineAsync();
        if (firstLine == null)
        {
            return;
        }

        string[] parts = firstLine.Split('|', 3);
        if (parts.Length < 2)
        {
            await writer.WriteLineAsync("BROKER_ACK|ERRO|FORMATO_INVALIDO");
            return;
        }

        string command = parts[0].Trim().ToUpperInvariant();

        if (command == "SUBSCRIBE")
        {
            await HandleSubscriberAsync(client, reader, writer, parts);
            return;
        }

        if (command == "PUBLISH")
        {
            await HandlePublishAsync(writer, parts);
            return;
        }

        await writer.WriteLineAsync("BROKER_ACK|ERRO|COMANDO_DESCONHECIDO");
    }
}

async Task HandleSubscriberAsync(TcpClient client, StreamReader reader, StreamWriter writer, string[] parts)
{
    if (parts.Length < 3)
    {
        await writer.WriteLineAsync("SUB_ACK|ERRO|FORMATO_INVALIDO");
        return;
    }

    string subscriberId = parts[1].Trim();
    string[] topics = parts[2]
        .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    if (string.IsNullOrWhiteSpace(subscriberId) || topics.Length == 0)
    {
        await writer.WriteLineAsync("SUB_ACK|ERRO|DADOS_INVALIDOS");
        return;
    }

    Subscriber subscriber = new(subscriberId, topics.ToList(), writer);
    subscribers[subscriberId] = subscriber;
    await writer.WriteLineAsync("SUB_ACK|SUCESSO");
    Console.WriteLine($"[PUBSUB] {subscriberId} subscreveu: {string.Join(", ", topics)}");
    await DeliverPendingMessagesAsync(subscriber);

    try
    {
        while (await reader.ReadLineAsync() != null)
        {
        }
    }
    finally
    {
        subscribers.TryRemove(subscriberId, out _);
        Console.WriteLine($"[PUBSUB] {subscriberId} desligado.");
    }
}

async Task HandlePublishAsync(StreamWriter writer, string[] parts)
{
    if (parts.Length < 3)
    {
        await writer.WriteLineAsync("PUB_ACK|ERRO|FORMATO_INVALIDO");
        return;
    }

    string topic = parts[1].Trim();
    string payload = parts[2];
    int delivered = await DeliverToMatchingSubscribersAsync(topic, payload);

    if (delivered == 0)
    {
        QueuePendingMessage(topic, payload);
        await writer.WriteLineAsync("PUB_ACK|SUCESSO|0|PENDENTE");
        Console.WriteLine($"[PUBSUB] {topic} guardado em fila pendente.");
        return;
    }

    await writer.WriteLineAsync($"PUB_ACK|SUCESSO|{delivered}");
    Console.WriteLine($"[PUBSUB] {topic} publicado para {delivered} subscritor(es).");
}

async Task<int> DeliverToMatchingSubscribersAsync(string topic, string payload)
{
    int delivered = 0;

    foreach (Subscriber subscriber in subscribers.Values)
    {
        if (!subscriber.Topics.Any(pattern => TopicMatches(pattern, topic)))
        {
            continue;
        }

        if (await TryDeliverToSubscriberAsync(subscriber, topic, payload))
        {
            delivered++;
        }
    }

    return delivered;
}

async Task DeliverPendingMessagesAsync(Subscriber subscriber)
{
    List<BrokerMessage> candidates;
    lock (pendingMessagesLock)
    {
        candidates = pendingMessages
            .Where(message => subscriber.Topics.Any(pattern => TopicMatches(pattern, message.Topic)))
            .OrderBy(message => message.Id)
            .ToList();
    }

    if (candidates.Count == 0)
    {
        return;
    }

    int delivered = 0;
    foreach (BrokerMessage message in candidates)
    {
        if (!await TryDeliverToSubscriberAsync(subscriber, message.Topic, message.Payload))
        {
            break;
        }

        delivered++;
        lock (pendingMessagesLock)
        {
            pendingMessages.RemoveAll(item => item.Id == message.Id);
        }
    }

    if (delivered > 0)
    {
        Console.WriteLine($"[PUBSUB] {delivered} mensagem(ns) pendente(s) entregue(s) a {subscriber.Id}.");
    }
}

async Task<bool> TryDeliverToSubscriberAsync(Subscriber subscriber, string topic, string payload)
{
    try
    {
        await subscriber.WriteLock.WaitAsync();
        try
        {
            await subscriber.Writer.WriteLineAsync($"MESSAGE|{topic}|{payload}");
        }
        finally
        {
            subscriber.WriteLock.Release();
        }

        return true;
    }
    catch
    {
        subscribers.TryRemove(subscriber.Id, out _);
        return false;
    }
}

void QueuePendingMessage(string topic, string payload)
{
    lock (pendingMessagesLock)
    {
        pendingMessages.Add(new BrokerMessage(++nextMessageId, topic, payload, DateTime.Now));
        if (pendingMessages.Count > MAX_PENDING_MESSAGES)
        {
            pendingMessages.RemoveRange(0, pendingMessages.Count - MAX_PENDING_MESSAGES);
        }
    }
}

bool TopicMatches(string pattern, string topic)
{
    string[] patternParts = pattern.Split('/');
    string[] topicParts = topic.Split('/');

    if (patternParts.Length != topicParts.Length)
    {
        return false;
    }

    for (int i = 0; i < patternParts.Length; i++)
    {
        if (patternParts[i] == "*")
        {
            continue;
        }

        if (!patternParts[i].Equals(topicParts[i], StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }
    }

    return true;
}

record Subscriber(string Id, List<string> Topics, StreamWriter Writer)
{
    public SemaphoreSlim WriteLock { get; } = new(1, 1);
}

record BrokerMessage(long Id, string Topic, string Payload, DateTime CreatedAt);
