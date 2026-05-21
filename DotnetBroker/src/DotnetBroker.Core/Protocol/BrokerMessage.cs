using System.Buffers.Binary;

namespace DotnetBroker.Core.Protocol;

// ---------------------------------------------------------------------------
// Payload records (immutable, allocation-friendly)
// ---------------------------------------------------------------------------

/// <summary>Mode a consumer wants to use when receiving messages.</summary>
public enum DeliveryMode : byte { Push = 0, Pull = 1 }

/// <summary>P_REG payload: producer registration.</summary>
public readonly record struct ProducerRegisterPayload(uint Topic, ushort Port)
{
    public static ProducerRegisterPayload Decode(ReadOnlySpan<byte> data) => new(
        Topic: BinaryPrimitives.ReadUInt32BigEndian(data[0..4]),
        Port:  BinaryPrimitives.ReadUInt16BigEndian(data[4..6]));

    public void Encode(Span<byte> buf)
    {
        BinaryPrimitives.WriteUInt32BigEndian(buf[0..4], Topic);
        BinaryPrimitives.WriteUInt16BigEndian(buf[4..6], Port);
    }
    public const int Size = 6;
}

/// <summary>C_REG payload: consumer registration.</summary>
public readonly record struct ConsumerRegisterPayload(uint Topic, ushort Port, uint GroupId, DeliveryMode Mode)
{
    public static ConsumerRegisterPayload Decode(ReadOnlySpan<byte> data) => new(
        Topic:   BinaryPrimitives.ReadUInt32BigEndian(data[0..4]),
        Port:    BinaryPrimitives.ReadUInt16BigEndian(data[4..6]),
        GroupId: BinaryPrimitives.ReadUInt32BigEndian(data[6..10]),
        Mode:    (DeliveryMode)data[10]);

    public void Encode(Span<byte> buf)
    {
        BinaryPrimitives.WriteUInt32BigEndian(buf[0..4], Topic);
        BinaryPrimitives.WriteUInt16BigEndian(buf[4..6], Port);
        BinaryPrimitives.WriteUInt32BigEndian(buf[6..10], GroupId);
        buf[10] = (byte)Mode;
    }
    public const int Size = 11;
}

/// <summary>PCM payload: a message going from producer → broker → consumer.</summary>
public readonly record struct ProduceConsumePayload(ushort ProducerPort, ulong Timestamp, byte[] Message)
{
    public static ProduceConsumePayload Decode(ReadOnlySpan<byte> data)
    {
        var port = BinaryPrimitives.ReadUInt16BigEndian(data[0..2]);
        var ts   = BinaryPrimitives.ReadUInt64BigEndian(data[2..10]);
        var msg  = data[10..].ToArray();
        return new(port, ts, msg);
    }

    public byte[] Encode()
    {
        var buf = new byte[2 + 8 + Message.Length];
        BinaryPrimitives.WriteUInt16BigEndian(buf.AsSpan(0, 2), ProducerPort);
        BinaryPrimitives.WriteUInt64BigEndian(buf.AsSpan(2, 8), Timestamp);
        Message.CopyTo(buf.AsSpan(10));
        return buf;
    }
}
