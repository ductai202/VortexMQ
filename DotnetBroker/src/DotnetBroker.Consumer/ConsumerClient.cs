using System.Net;
using System.Net.Sockets;
using DotnetBroker.Core.Protocol;

namespace DotnetBroker.Consumer;

/// <summary>
/// Consumer client:
/// 1. Opens own TCP server on <port>
/// 2. Connects to KAdmin and sends C_REG
/// 3. Accepts KAdmin's reverse connection
/// 4. Receives PCM messages; sends ACK
/// In Pull mode, also sends C_RD to signal readiness before each message.
/// </summary>
public sealed class ConsumerClient(ushort localPort, uint topicId, uint groupId, DeliveryMode mode,
    string adminHost = "127.0.0.1", int adminPort = 10000)
{
    private NetworkStream? _brokerStream; // stream from KAdmin (messages arrive here)

    public async Task ConnectAsync(CancellationToken ct = default)
    {
        // Step 1: Open our own TCP server
        var listener = new TcpListener(IPAddress.Any, localPort);
        listener.Start();
        Console.WriteLine($"[Consumer] Listening for KAdmin callback on port {localPort}");

        // Step 2: Connect to KAdmin
        using var adminClient = new TcpClient();
        adminClient.NoDelay = true;
        await adminClient.ConnectAsync(adminHost, adminPort, ct);
        var adminStream = adminClient.GetStream();
        Console.WriteLine($"[Consumer] Connected to KAdmin at {adminHost}:{adminPort}");

        // Step 3: Send C_REG
        var reg = new ConsumerRegisterPayload(Topic: topicId, Port: localPort, GroupId: groupId, Mode: mode);
        var regBuf = new byte[ConsumerRegisterPayload.Size];
        reg.Encode(regBuf);
        await adminStream.WriteRawBytesAsync(MessageType.C_Reg, regBuf, ct);
        Console.WriteLine($"[Consumer] Sent C_REG: topic={topicId} group={groupId} port={localPort} mode={mode}");

        // Step 4: KAdmin connects back
        var callback = await listener.AcceptTcpClientAsync(ct);
        callback.NoDelay = true;
        listener.Stop();
        _brokerStream = callback.GetStream();

        // Step 5: Read R_C_REG ACK
        var (ackType, ackPayload) = await _brokerStream.ReadMessageAsync(ct);
        if (ackType == MessageType.R_C_Reg && ackPayload.Length > 0 && ackPayload[0] == 0)
            Console.WriteLine("[Consumer] Registered successfully with KAdmin.");
        else
            Console.WriteLine($"[Consumer] Registration response: type={ackType}");
    }

    /// <summary>Receive messages in a loop, printing to stdout.</summary>
    public async Task ReceiveLoopAsync(Action<ProduceConsumePayload>? onMessage = null, CancellationToken ct = default)
    {
        if (_brokerStream is null) throw new InvalidOperationException("Not connected — call ConnectAsync first.");
        Console.WriteLine($"[Consumer] Receiving messages (mode={mode})...");

        while (!ct.IsCancellationRequested)
        {
            // Pull mode: signal readiness first
            if (mode == DeliveryMode.Pull)
            {
                await _brokerStream.WriteAckAsync(MessageType.C_Rd, 1, ct);
                var (rdAck, _) = await _brokerStream.ReadMessageAsync(ct);
                if (rdAck != MessageType.R_C_Rd)
                    Console.WriteLine($"[Consumer] Expected R_C_RD, got {rdAck}");
            }

            // Wait for PCM
            var (msgType, payload) = await _brokerStream.ReadMessageAsync(ct);
            if (msgType != MessageType.Pcm)
            {
                Console.WriteLine($"[Consumer] Unexpected message type {msgType}");
                continue;
            }

            var pcm = ProduceConsumePayload.Decode(payload);
            var text = System.Text.Encoding.UTF8.GetString(pcm.Message);
            Console.WriteLine($"[Consumer-{groupId}] Received: \"{text}\" (ts={pcm.Timestamp} from port={pcm.ProducerPort})");

            if (onMessage is not null)
                onMessage(pcm);

            // Send ACK
            await _brokerStream.WriteAckAsync(MessageType.R_Pcm, 0, ct);
        }
    }
}
