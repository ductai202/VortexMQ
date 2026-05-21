# DotnetBroker — Wire Protocol Specification

## Frame Layout

Every message on the wire follows this exact layout:

```
Byte 0        Byte 1        Bytes 2..N
┌─────────────┬─────────────┬──────────────────────┐
│ Length (u8) │ Type   (u8) │ Payload (0..253 bytes)│
└─────────────┴─────────────┴──────────────────────┘
```

- **Length** = number of bytes that follow the `Length` byte itself. This equals `1 (Type) + len(Payload)`.
- **Type** = `MessageType` enum value (see table below).
- **Max payload** = 253 bytes (255 − 1 length byte − 1 type byte).

## Message Types

| Decimal | Hex | Name       | Direction              | Payload Layout |
|---------|-----|------------|------------------------|----------------|
| 1       | 0x01 | `ECHO`    | Client→Server          | UTF-8 string   |
| 2       | 0x02 | `P_REG`   | Producer→Admin         | See P_REG Payload |
| 3       | 0x03 | `C_REG`   | Consumer→Admin         | See C_REG Payload |
| 4       | 0x04 | `PCM`     | Producer↔Admin↔Consumer | See PCM Payload |
| 5       | 0x05 | `C_RD`    | Consumer→Admin         | 1 byte (ready signal, value=1) |
| 101     | 0x65 | `R_ECHO`  | Server→Client          | UTF-8 string (echo back) |
| 102     | 0x66 | `R_P_REG` | Admin→Producer         | 1 byte ACK (0=ok, 1=error) |
| 103     | 0x67 | `R_C_REG` | Admin→Consumer         | 1 byte ACK (0=ok, 1=error) |
| 104     | 0x68 | `R_PCM`   | Consumer→Admin         | 1 byte ACK (0=ok) |
| 105     | 0x69 | `R_C_RD`  | Admin→Consumer         | 1 byte ACK (0=ok) |

## Payload Definitions

### P_REG Payload (6 bytes)

```
Bytes 0–3    Bytes 4–5
┌──────────┬──────────┐
│ Topic    │ Port     │
│ uint32 BE│ uint16 BE│
└──────────┴──────────┘
```

- `Topic`: Numeric topic ID (big-endian uint32)
- `Port`: Producer's callback TCP port (big-endian uint16)

### C_REG Payload (11 bytes)

```
Bytes 0–3    Bytes 4–5    Bytes 6–9    Byte 10
┌──────────┬──────────┬──────────┬──────────┐
│ Topic    │ Port     │ GroupId  │ Mode     │
│ uint32 BE│ uint16 BE│ uint32 BE│ uint8    │
└──────────┴──────────┴──────────┴──────────┘
```

- `Topic`: Numeric topic ID (big-endian uint32)
- `Port`: Consumer's callback TCP port (big-endian uint16)
- `GroupId`: Consumer group ID (big-endian uint32)
- `Mode`: `0` = Push, `1` = Pull

### PCM Payload (10 + N bytes)

```
Bytes 0–1    Bytes 2–9      Bytes 10..N
┌──────────┬──────────────┬──────────────────┐
│ ProdPort │ Timestamp    │ Message body     │
│ uint16 BE│ uint64 BE ms │ raw bytes        │
└──────────┴──────────────┴──────────────────┘
```

- `ProdPort`: Producer's callback port (identifies source, big-endian uint16)
- `Timestamp`: Unix epoch milliseconds at time of send (big-endian uint64)
- `Message`: Opaque bytes (application payload). Max 243 bytes (253 − 10 header).

### ACK Payload (1 byte)

Used by `R_P_REG`, `R_C_REG`, `R_PCM`, `R_C_RD`, `ECHO`:

```
Byte 0
┌──────────┐
│ ACK code │
└──────────┘
```

- `0` = success
- `1` = error (e.g., ring buffer full for `R_P_REG`)

## Full Message Examples

### ECHO request/response (message "hi")

```
Request:
  Length=3  Type=0x01  Payload=0x68 0x69
  │ 03 │ 01 │ 68 69 │

Response:
  Length=3  Type=0x65  Payload=0x68 0x69
  │ 03 │ 65 │ 68 69 │
```

### P_REG (topic=1, port=10001)

```
Length=7  Type=0x02  Payload:
  Topic  = 0x00 0x00 0x00 0x01
  Port   = 0x27 0x11
  │ 07 │ 02 │ 00 00 00 01  27 11 │
```

### PCM (producerPort=10001, ts=1234567890000, msg="hello")

```
Length=16  Type=0x04  Payload:
  Port = 0x27 0x11
  TS   = 0x00 0x00 0x01 0x1F 0x71 0xFB 0x04 0x10
  Msg  = 0x68 0x65 0x6C 0x6C 0x6F
  │ 10 │ 04 │ 27 11  00 00 01 1F 71 FB 04 10  68 65 6C 6C 6F │
```

## Endianness

All multi-byte integers are **big-endian** (network byte order), encoded using `System.Buffers.Binary.BinaryPrimitives`:

```csharp
// Write
BinaryPrimitives.WriteUInt32BigEndian(span, value);
BinaryPrimitives.WriteUInt64BigEndian(span, value);

// Read
var value = BinaryPrimitives.ReadUInt32BigEndian(span);
var value = BinaryPrimitives.ReadUInt64BigEndian(span);
```

## Limitations

- **Max payload**: 253 bytes (1-byte length field). Future: switch to 4-byte length for larger messages.
- **No message versioning**: The protocol has no version field; breaking changes require a new implementation.
- **No compression**: Messages are stored and transmitted as raw bytes.
- **No authentication**: Any TCP client can register as a producer or consumer.
