using System.Net.Sockets;

namespace DotnetBroker.Core.Protocol;

/// <summary>
/// Read/write helpers on top of a raw NetworkStream.
/// Wire format:  [ Length:u8 | Type:u8 | Payload:Length-1 bytes ]
/// Length = number of bytes that follow the Length byte itself (i.e. Type + Payload).
/// Max payload = 253 bytes (255 - 1 type byte - 1 length byte... actually 254 - 1 = 253).
/// </summary>
public static class StreamExtensions
{
    // ----- Write -----

    public static Task WriteAckAsync(this NetworkStream stream, MessageType type, byte ack = 0,
        CancellationToken ct = default)
        => WriteRawAsync(stream, type, [ack], ct);

    public static Task WriteRawBytesAsync(this NetworkStream stream, MessageType type, byte[] payload,
        CancellationToken ct = default)
        => WriteRawAsync(stream, type, payload, ct);

    public static Task WriteStringAsync(this NetworkStream stream, MessageType type, string text,
        CancellationToken ct = default)
        => WriteRawAsync(stream, type, System.Text.Encoding.UTF8.GetBytes(text), ct);

    private static async Task WriteRawAsync(NetworkStream stream, MessageType type, byte[] payload,
        CancellationToken ct)
    {
        var frame = new byte[1 + 1 + payload.Length]; // length + type + payload
        frame[0] = (byte)(1 + payload.Length);        // length = type(1) + payload
        frame[1] = (byte)type;
        payload.CopyTo(frame, 2);
        await stream.WriteAsync(frame, ct);
    }

    // ----- Read -----

    /// <summary>Returns (MessageType, payload bytes) or throws on EOF.</summary>
    public static async Task<(MessageType Type, byte[] Payload)> ReadMessageAsync(
        this NetworkStream stream, CancellationToken ct = default)
    {
        // Read the 1-byte length header
        var lenBuf = new byte[1];
        await ReadExactAsync(stream, lenBuf, ct);
        var totalLen = lenBuf[0]; // includes the type byte

        // Read type + payload
        var body = new byte[totalLen];
        await ReadExactAsync(stream, body, ct);

        var msgType = (MessageType)body[0];
        var payload = body[1..];
        return (msgType, payload);
    }

    private static async Task ReadExactAsync(NetworkStream stream, byte[] buf, CancellationToken ct)
    {
        var read = 0;
        while (read < buf.Length)
        {
            var n = await stream.ReadAsync(buf.AsMemory(read), ct);
            if (n == 0) throw new EndOfStreamException("Connection closed by remote.");
            read += n;
        }
    }
}
