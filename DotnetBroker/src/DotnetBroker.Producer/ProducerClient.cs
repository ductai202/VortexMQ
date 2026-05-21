using System.Net;
using System.Net.Sockets;
using DotnetBroker.Core.Protocol;

namespace DotnetBroker.Producer;

/// <summary>
/// Producer client:
/// 1. Opens own TCP server on <port>
/// 2. Connects to KAdmin and sends P_REG
/// 3. Accepts KAdmin's reverse connection
/// 4. Sends PCM messages from stdin or programmatically
/// </summary>
public sealed class ProducerClient(ushort localPort, uint topicId, string adminHost = "127.0.0.1", int adminPort = 10000)
{
    private NetworkStream? _adminStream; // stream to KAdmin's callback connection

    public async Task ConnectAsync(CancellationToken ct = default)
    {
        // Step 1: Start our own TCP server so KAdmin can connect back
        var listener = new TcpListener(IPAddress.Any, localPort);
        listener.Start();
        Console.WriteLine($"[Producer] Listening for KAdmin callback on port {localPort}");

        // Step 2: Connect to KAdmin's admin port
        using var adminClient = new TcpClient();
        adminClient.NoDelay = true;
        await adminClient.ConnectAsync(adminHost, adminPort, ct);
        var adminStream = adminClient.GetStream();
        Console.WriteLine($"[Producer] Connected to KAdmin at {adminHost}:{adminPort}");

        // Step 3: Send P_REG
        var reg = new ProducerRegisterPayload(Topic: topicId, Port: localPort);
        var regBuf = new byte[ProducerRegisterPayload.Size];
        reg.Encode(regBuf);
        await adminStream.WriteRawBytesAsync(MessageType.P_Reg, regBuf, ct);
        Console.WriteLine($"[Producer] Sent P_REG: topic={topicId} port={localPort}");

        // Step 4: KAdmin connects back to our listener
        var callback = await listener.AcceptTcpClientAsync(ct);
        callback.NoDelay = true;
        listener.Stop();
        _adminStream = callback.GetStream();

        // Step 5: Read R_P_REG ACK from KAdmin (sent through the callback channel)
        var (ackType, ackPayload) = await _adminStream.ReadMessageAsync(ct);
        if (ackType == MessageType.R_P_Reg && ackPayload.Length > 0 && ackPayload[0] == 0)
            Console.WriteLine("[Producer] Registered successfully with KAdmin.");
        else
            Console.WriteLine($"[Producer] Registration response: type={ackType}");
    }

    /// <summary>Send a PCM message to the broker.</summary>
    public async Task SendMessageAsync(byte[] messageBody, CancellationToken ct = default)
    {
        if (_adminStream is null) throw new InvalidOperationException("Not connected — call ConnectAsync first.");
        var pcm = new ProduceConsumePayload(
            ProducerPort: localPort,
            Timestamp:    (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Message:      messageBody);

        await _adminStream.WriteRawBytesAsync(MessageType.Pcm, pcm.Encode(), ct);

        // Read R_PCM ACK
        var (ackType, _) = await _adminStream.ReadMessageAsync(ct);
        if (ackType != MessageType.R_Pcm)
            Console.WriteLine($"[Producer] Expected R_PCM but got {ackType}");
    }

    /// <summary>Run interactively: read lines from stdin and send as messages.</summary>
    public async Task RunInteractiveAsync(CancellationToken ct = default)
    {
        Console.WriteLine("[Producer] Ready. Type messages and press Enter. Ctrl+C to quit.");
        while (!ct.IsCancellationRequested)
        {
            var line = await Task.Run(() => Console.ReadLine(), ct);
            if (line is null) break;
            var bytes = System.Text.Encoding.UTF8.GetBytes(line);
            await SendMessageAsync(bytes, ct);
            Console.WriteLine($"[Producer] Sent: {line}");
        }
    }
}
