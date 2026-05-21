using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using DotnetBroker.Core.Models;
using DotnetBroker.Core.Protocol;

namespace DotnetBroker.Server;

/// <summary>
/// KAdmin — the central message broker server.
/// Listens on port 10000 for P_REG and C_REG commands.
/// Manages topics and consumer groups.
/// Spawns ProducerHandler and ConsumerGroupAdvancer tasks per connection.
/// </summary>
public sealed class BrokerServer
{
    private readonly int _adminPort;
    private readonly ConcurrentDictionary<uint, Topic> _topics = new();
    private readonly PersistenceManager _persistence;
    private CancellationTokenSource _cts = new();

    public BrokerServer(int adminPort = 10000, string? persistDir = null)
    {
        _adminPort = adminPort;
        _persistence = new PersistenceManager(persistDir ?? "broker_data", _topics);
    }

    public async Task RunAsync(CancellationToken externalCt = default)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(externalCt);
        _cts = linked;
        var ct = linked.Token;

        // Restore persisted state
        await _persistence.RestoreAsync();

        var listener = new TcpListener(IPAddress.Any, _adminPort);
        listener.Start();
        Console.WriteLine($"[KAdmin] Listening on port {_adminPort}");

        // Periodic snapshot
        _ = Task.Run(() => PeriodicSnapshotAsync(ct), ct);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                var client = await listener.AcceptTcpClientAsync(ct);
                _ = Task.Run(() => HandleAdminConnectionAsync(client, ct), ct);
            }
        }
        catch (OperationCanceledException) { }
        finally
        {
            listener.Stop();
            await _persistence.SnapshotAsync();
            Console.WriteLine("[KAdmin] Stopped.");
        }
    }

    private async Task HandleAdminConnectionAsync(TcpClient client, CancellationToken ct)
    {
        client.NoDelay = true;
        var stream = client.GetStream();
        var remoteEp = client.Client.RemoteEndPoint;
        Console.WriteLine($"[KAdmin] Admin connection from {remoteEp}");
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var (msgType, payload) = await stream.ReadMessageAsync(ct);
                switch (msgType)
                {
                    case MessageType.Echo:
                        var text = System.Text.Encoding.UTF8.GetString(payload);
                        Console.WriteLine($"[KAdmin] ECHO: {text}");
                        await stream.WriteStringAsync(MessageType.R_Echo, text, ct);
                        break;

                    case MessageType.P_Reg:
                        await HandleProducerRegAsync(payload, ct);
                        // R_P_REG is sent via the callback connection in HandleProducerRegAsync
                        break;

                    case MessageType.C_Reg:
                        await HandleConsumerRegAsync(payload, ct);
                        // R_C_REG is sent via the callback connection in HandleConsumerRegAsync
                        break;

                    default:
                        Console.WriteLine($"[KAdmin] Unexpected message type {msgType} on admin port — ignoring.");
                        break;
                }
            }
        }
        catch (EndOfStreamException) { Console.WriteLine($"[KAdmin] Admin client {remoteEp} disconnected."); }
        catch (IOException ex) when (ex.InnerException is SocketException) { Console.WriteLine($"[KAdmin] Admin socket error: {ex.Message}"); }
        catch (OperationCanceledException) { }
        finally { client.Dispose(); }
    }

    // ---- Producer Registration ----

    private async Task HandleProducerRegAsync(byte[] payload, CancellationToken ct)
    {
        var reg = ProducerRegisterPayload.Decode(payload);
        Console.WriteLine($"[KAdmin] P_REG: topic={reg.Topic} producerPort={reg.Port}");

        var topic = GetOrAddTopic(reg.Topic);

        // Connect back to producer's TCP server
        var producerClient = new TcpClient();
        producerClient.NoDelay = true;
        await producerClient.ConnectAsync(IPAddress.Loopback, reg.Port, ct);
        Console.WriteLine($"[KAdmin] Connected back to producer on port {reg.Port}");

        var producerStream = producerClient.GetStream();

        // Send R_P_REG ACK via the callback connection (broker → producer)
        await producerStream.WriteAckAsync(MessageType.R_P_Reg, 0, ct);
        Console.WriteLine($"[KAdmin] Sent R_P_REG ACK to producer port={reg.Port}");

        // Start handler to read PCM messages from this producer
        _ = Task.Run(() => new ProducerHandler(topic, reg.Port, producerStream, producerClient).RunAsync(ct), ct);

        // After connecting, save snapshot
        _ = _persistence.SnapshotAsync();
    }

    // ---- Consumer Registration ----

    private async Task HandleConsumerRegAsync(byte[] payload, CancellationToken ct)
    {
        var reg = ConsumerRegisterPayload.Decode(payload);
        Console.WriteLine($"[KAdmin] C_REG: topic={reg.Topic} group={reg.GroupId} port={reg.Port} mode={reg.Mode}");

        var topic = GetOrAddTopic(reg.Topic);
        var group = await topic.GetOrAddGroupAsync(reg.GroupId, reg.Mode);

        // Connect back to consumer's TCP server
        var consumerClient = new TcpClient();
        consumerClient.NoDelay = true;
        await consumerClient.ConnectAsync(IPAddress.Loopback, reg.Port, ct);
        Console.WriteLine($"[KAdmin] Connected back to consumer on port {reg.Port} (group {reg.GroupId})");

        var consumerStream = consumerClient.GetStream();

        // Send R_C_REG ACK via the callback connection (broker → consumer)
        await consumerStream.WriteAckAsync(MessageType.R_C_Reg, 0, ct);
        Console.WriteLine($"[KAdmin] Sent R_C_REG ACK to consumer port={reg.Port} group={reg.GroupId}");

        await group.AddConsumerAsync(reg.Port, consumerStream);

        // If this is the first consumer in a group, start the advancer for that group
        if (group.ConsumerCount == 1)
        {
            _ = Task.Run(() => new ConsumerGroupAdvancer(group, topic.Queue).RunAsync(ct), ct);
            Console.WriteLine($"[KAdmin] Started ConsumerGroupAdvancer for group {reg.GroupId}");
        }

        // Handle incoming messages from consumer (C_RD in Pull mode)
        // Note: C_RD is handled directly by ConsumerGroupAdvancer reading the stream.
        // No separate reader is needed; the advancer owns the consumer stream.

        _ = _persistence.SnapshotAsync();
    }

    private async Task HandleConsumerMessagesAsync(ConsumerGroup group, NetworkStream stream, CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var (msgType, _) = await stream.ReadMessageAsync(ct);
                if (msgType == MessageType.C_Rd && group.Mode == DeliveryMode.Pull)
                {
                    // Consumer signals readiness — put into ready queue
                    await group.ReadyQueue.Writer.WriteAsync(1, ct);
                    await stream.WriteAckAsync(MessageType.R_C_Rd, 0, ct);
                }
            }
        }
        catch (EndOfStreamException) { }
        catch (IOException) { }
        catch (OperationCanceledException) { }
    }

    // ---- Topic management ----

    private Topic GetOrAddTopic(uint topicId)
        => _topics.GetOrAdd(topicId, id => new Topic(id));

    public IReadOnlyDictionary<uint, Topic> Topics => _topics;

    // ---- Periodic Snapshot ----
    private async Task PeriodicSnapshotAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(30), ct);
                await _persistence.SnapshotAsync();
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex) { Console.WriteLine($"[KAdmin] Snapshot error: {ex.Message}"); }
        }
    }
}
