# DotnetBroker — Technology & Skills Self-Learning Guide

This document explains every technology and concept used in this project, with enough depth to learn each from scratch.

---

## 1. C# / .NET 8 Language Features

### Records and `readonly record struct`
Used in `BrokerMessage.cs` for `ProducerRegisterPayload`, `ConsumerRegisterPayload`, `ProduceConsumePayload`.

```csharp
// Record struct: immutable, value-type, auto-generated Equals/GetHashCode/ToString
public readonly record struct ProducerRegisterPayload(uint Topic, ushort Port);

var a = new ProducerRegisterPayload(1, 10001);
var b = new ProducerRegisterPayload(1, 10001);
Console.WriteLine(a == b); // true — structural equality
```

**Learn more**: [C# Records](https://learn.microsoft.com/en-us/dotnet/csharp/language-reference/builtin-types/record), [Value types vs Reference types](https://learn.microsoft.com/en-us/dotnet/csharp/language-reference/keywords/value-types)

### Primary Constructors
Used in all model classes (`Topic`, `ConsumerGroup`, etc.)

```csharp
// Traditional
public class BrokerServer { private readonly int _port; public BrokerServer(int port) { _port = port; } }

// Primary constructor (C# 12)
public sealed class BrokerServer(int adminPort = 10000) { /* adminPort available as field */ }
```

### Nullable Reference Types
The `#nullable enable` setting makes the compiler track nullability:

```csharp
string? name = null;   // nullable — compiler allows null
string  name2 = null;  // compiler warning! non-nullable must not be null
```

### Pattern Matching
```csharp
// Type pattern
if (result is null) { ... }

// Switch expression
var msg = msgType switch {
    MessageType.Echo  => "echo",
    MessageType.P_Reg => "producer",
    _                 => "unknown"
};
```

**Learn more**: [Pattern matching](https://learn.microsoft.com/en-us/dotnet/csharp/fundamentals/functional/pattern-matching)

### Collection Expressions (C# 12)
```csharp
List<int> list = [];          // empty list
byte[] buf = [0x01, 0x02];   // array literal
var copy = [..existing];     // spread operator
```

---

## 2. Async/Await and the Task Parallel Library (TPL)

### How `async/await` works
```csharp
// This does NOT block a thread while waiting for I/O
public async Task<string> ReadLineAsync(NetworkStream stream)
{
    var buf = new byte[256];
    var n = await stream.ReadAsync(buf);  // releases thread while waiting
    return Encoding.UTF8.GetString(buf, 0, n);
}
```

The compiler transforms `async` methods into a **state machine**. The `await` keyword suspends the method and returns the thread to the thread pool, which can then serve other requests.

### `Task` vs `ValueTask`
- `Task<T>`: always allocates a heap object. Use for infrequent operations.
- `ValueTask<T>`: allocates only when the result isn't immediately available. Use for hot paths.

```csharp
// Prefer ValueTask for frequently-called methods that often complete synchronously
public ValueTask<int> ReadFromCacheAsync(int key) { ... }
```

### Fire-and-Forget Tasks
```csharp
// Don't await — start background work, don't block current method
_ = Task.Run(() => SomeLongOperation(ct), ct);
```

**Warning**: Always pass a `CancellationToken`. Unhandled exceptions in fire-and-forget tasks are lost unless you add `.ContinueWith(t => Log(t.Exception))`.

### CancellationToken Pattern
```csharp
using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

// Pass token everywhere
await server.RunAsync(cts.Token);
```

**Learn more**: [Async programming](https://learn.microsoft.com/en-us/dotnet/csharp/asynchronous-programming/), [TAP pattern](https://learn.microsoft.com/en-us/dotnet/standard/asynchronous-programming-patterns/task-based-asynchronous-pattern-tap)

---

## 3. TCP Networking with `System.Net.Sockets`

### Server: TcpListener
```csharp
var listener = new TcpListener(IPAddress.Any, port);
listener.Start();

while (true)
{
    var client = await listener.AcceptTcpClientAsync(ct);  // waits for connection
    _ = Task.Run(() => HandleClient(client, ct));           // handle concurrently
}
```

### Client: TcpClient
```csharp
var client = new TcpClient();
client.NoDelay = true;  // disable Nagle algorithm for low latency
await client.ConnectAsync("127.0.0.1", 10000, ct);
var stream = client.GetStream();
```

### The "Nagle Algorithm" and `NoDelay`
TCP normally buffers small writes and sends them together (Nagle algorithm) to improve bandwidth efficiency. For message brokers, this adds latency. Setting `NoDelay = true` disables it:

```csharp
client.NoDelay = true;  // send each write immediately (disable Nagle)
```

### NetworkStream Read/Write
```csharp
// Write
await stream.WriteAsync(data.AsMemory(0, length), ct);

// Read — must handle partial reads!
var totalRead = 0;
while (totalRead < expectedBytes)
{
    var n = await stream.ReadAsync(buf.AsMemory(totalRead), ct);
    if (n == 0) throw new EndOfStreamException();
    totalRead += n;
}
```

**Why partial reads?** TCP is a byte stream, not a message stream. A single `ReadAsync` may return fewer bytes than expected. Always read in a loop until you have all expected bytes.

**Learn more**: [TcpListener](https://learn.microsoft.com/en-us/dotnet/api/system.net.sockets.tcplistener), [NetworkStream](https://learn.microsoft.com/en-us/dotnet/api/system.net.sockets.networkstream)

---

## 4. Binary Serialization with `BinaryPrimitives`

### Why Binary Instead of JSON?
- JSON `{"topic":1,"port":10001}` = ~22 bytes
- Binary P_REG = 6 bytes — 73% smaller
- Binary parsing is allocation-free; JSON parsing allocates strings

### Endianness
Network protocols use **big-endian** (most significant byte first):
- `uint32` value `0x00000001` → bytes `[0x00, 0x00, 0x00, 0x01]`
- `uint16` value `0x2711` (10001) → bytes `[0x27, 0x11]`

```csharp
using System.Buffers.Binary;

// Write big-endian
var buf = new byte[4];
BinaryPrimitives.WriteUInt32BigEndian(buf, 42);   // buf = [0,0,0,42]

// Read big-endian
var value = BinaryPrimitives.ReadUInt32BigEndian(buf);  // 42

// Span-based — no allocation
void Encode(Span<byte> buf)
{
    BinaryPrimitives.WriteUInt32BigEndian(buf[0..4], Topic);
    BinaryPrimitives.WriteUInt16BigEndian(buf[4..6], Port);
}
```

**Learn more**: [BinaryPrimitives](https://learn.microsoft.com/en-us/dotnet/api/system.buffers.binary.binaryprimitives), [Spans in .NET](https://learn.microsoft.com/en-us/dotnet/standard/memory-and-spans/)

---

## 5. Concurrency Primitives

### `ReaderWriterLockSlim`
Allows **multiple concurrent readers** OR **one exclusive writer**. Perfect for a queue where many consumer groups peek simultaneously but only one producer writes:

```csharp
private readonly ReaderWriterLockSlim _lock = new();

// Multiple threads can read simultaneously
public T? Peek(long offset)
{
    _lock.EnterReadLock();
    try { return _arr[offset]; }
    finally { _lock.ExitReadLock(); }
}

// Only one thread can write at a time
public void PushBack(T item)
{
    _lock.EnterWriteLock();
    try { _arr[_tail++] = item; }
    finally { _lock.ExitWriteLock(); }
}
```

### `SemaphoreSlim`
An async-compatible mutex. Unlike `lock`, it can be `await`ed:

```csharp
private readonly SemaphoreSlim _lock = new(1, 1);  // binary semaphore (mutex)

public async Task AddAsync(T item)
{
    await _lock.WaitAsync();  // won't block a thread (uses await)
    try { _list.Add(item); }
    finally { _lock.Release(); }
}
```

### `ConcurrentDictionary<K,V>`
Thread-safe dictionary — no explicit locking needed:

```csharp
var topics = new ConcurrentDictionary<uint, Topic>();
var topic = topics.GetOrAdd(topicId, id => new Topic(id));  // atomic
```

### `Interlocked.Increment`
Atomic 64-bit increment — no lock needed for a simple counter:

```csharp
public long Offset;  // shared field

// In ConsumerGroupAdvancer:
Interlocked.Increment(ref group.Offset);  // atomic, safe from multiple threads
```

### `System.Threading.Channels.Channel<T>`
A high-performance async producer-consumer queue built into .NET:

```csharp
// Unbounded: writer never blocks
var ch = Channel.CreateUnbounded<int>();

// Write (from any thread)
await ch.Writer.WriteAsync(signal, ct);

// Read (in advancer — blocks until signal arrives)
var signal = await ch.Reader.ReadAsync(ct);
```

**Learn more**: [Channels](https://learn.microsoft.com/en-us/dotnet/core/extensions/channels), [Threading in .NET](https://learn.microsoft.com/en-us/dotnet/standard/threading/)

---

## 6. Ring Buffer / Circular Buffer Data Structure

A ring buffer is a fixed-size array used as a circular queue:

```
Capacity = 4:
Index:   [0] [1] [2] [3]
Data:    [A] [B] [C] [ ]
          ↑head=0     ↑tail=3, count=3

After PopFront():
         [_] [B] [C] [ ]
              ↑head=1  ↑tail=3, count=2, PopCount=1

After PushBack(D):
         [_] [B] [C] [D]
              ↑head=1  ↑tail=0 (wraps!), count=3
```

**Key math**:
- `_tail = (_tail + 1) % _capacity` — modulo for wrap-around
- `Peek(absoluteOffset)` → `relativePos = absoluteOffset - PopCount` → `arr[(head + relativePos) % cap]`

This enables multiple consumer groups to peek at different positions independently without copying data.

**Learn more**: [Ring buffer](https://en.wikipedia.org/wiki/Circular_buffer)

---

## 7. Message Queue / Broker Concepts

### Why Message Queues?
- **Decoupling**: Producer doesn't know about consumers
- **Buffering**: Handle bursts without losing messages
- **Fan-out**: One producer, many consumers

### Consumer Groups
A consumer group is a logical subscriber. Each group gets a **copy** of every message (independent offsets). Within a group, messages are split across consumers (load balancing):

```
Topic:   [MSG_1] [MSG_2] [MSG_3] [MSG_4]

Group A (offset=0): Gets all messages
Group B (offset=0): Gets all messages (independently)

Within Group A, 2 consumers:
  Consumer A1: MSG_1, MSG_3 (round-robin)
  Consumer A2: MSG_2, MSG_4
```

### Push vs Pull Delivery
| Mode | Who Controls Rate | Latency | Backpressure |
|------|------------------|---------|-------------|
| Push | Server | Lower | Consumer can overflow |
| Pull | Consumer | Higher | Consumer controls its own rate |

### Offset-Based Storage
Instead of deleting messages after delivery, we keep messages in a ring buffer and track the "read position" (offset) per consumer group. Messages are only freed when ALL groups have consumed them.

### Exponential Backoff
When a consumer is idle (no new messages), polling the queue continuously wastes CPU:

```csharp
int backoffMs = 10;  // start at 10ms
while (no_message) {
    await Task.Delay(backoffMs);
    backoffMs = Math.Min(backoffMs * 2, 640);  // 10→20→40→80→160→320→640ms
}
backoffMs = 10;  // reset on success
```

---

## 8. Testing with xUnit and FluentAssertions

### Unit Tests
Test a single class in isolation, with no external dependencies:

```csharp
[Fact]  // single test
public void PushBack_And_PopFront_MaintainsFifoOrder()
{
    var q = new RingBufferQueue<int>(5);
    q.PushBack(10); q.PushBack(20);
    q.PopFront().Should().Be(10);  // FluentAssertions
}

[Theory]  // parameterized test
[InlineData(1, 10001, 100, DeliveryMode.Push)]
[InlineData(2, 10002, 200, DeliveryMode.Pull)]
public void ConsumerReg_RoundTrips(uint topic, int port, uint group, DeliveryMode mode)
{
    // ...
}
```

### Integration Tests
Test the full system end-to-end. Key pattern: **spin up a real server** in test setup, tear it down in `DisposeAsync`:

```csharp
public class MyTest : IAsyncDisposable
{
    private readonly BrokerFixture _broker = new();  // starts server

    [Fact]
    public async Task MyFlow_Works()
    {
        // connect real clients to _broker.AdminPort
        // send messages, assert reception
    }

    public async ValueTask DisposeAsync() => await _broker.DisposeAsync();
}
```

### FluentAssertions Syntax

```csharp
result.Should().Be(42);
result.Should().NotBeNull();
list.Should().HaveCount(3);
list.Should().Contain("hello");
act.Should().Throw<InvalidOperationException>().WithMessage("*full*");
await act.Should().NotThrowAsync();
```

**Learn more**: [xUnit](https://xunit.net/), [FluentAssertions](https://fluentassertions.com/)

---

## 9. BenchmarkDotNet

BenchmarkDotNet is the standard for statistically rigorous .NET benchmarks. It handles JIT warmup, garbage collection, and statistical analysis:

```csharp
[MemoryDiagnoser]   // measure allocations
[SimpleJob]         // run multiple iterations
public class MyBenchmark
{
    [Params(1000, 10000)]  // run for each value
    public int N;

    [GlobalSetup]  // run once before all benchmarks
    public void Setup() { /* prepare data */ }

    [Benchmark]
    public void MyOperation()
    {
        for (var i = 0; i < N; i++)
            DoSomething();
    }
}
```

**Run**: `dotnet run -c Release` (must be Release mode!)

**Output**:
```
| Method       | N     | Mean    | Alloc |
|------------- |------ |--------:|------:|
| MyOperation  | 1,000 | 8.23 µs | 0 B   |
| MyOperation  | 10000 | 82.1 µs | 0 B   |
```

**Learn more**: [BenchmarkDotNet](https://benchmarkdotnet.org/)

---

## 10. Persistence: JSON + Binary Files

### System.Text.Json
Built-in JSON serializer:

```csharp
// Serialize
var json = JsonSerializer.Serialize(obj, new JsonSerializerOptions { WriteIndented = true });
await File.WriteAllTextAsync("snapshot.json", json);

// Deserialize
var obj = JsonSerializer.Deserialize<MyType>(json);
```

### Binary Offset Journal
A simple binary file — just store the offset as 8 bytes (Int64):

```csharp
// Write
var buf = new byte[8];
BinaryPrimitives.WriteInt64BigEndian(buf, offset);
await using var fs = new FileStream(path, FileMode.Create);
await fs.WriteAsync(buf);

// Read
var buf = new byte[8];
await fs.ReadAsync(buf);
var offset = BinaryPrimitives.ReadInt64BigEndian(buf);
```

**Learn more**: [System.Text.Json](https://learn.microsoft.com/en-us/dotnet/standard/serialization/system-text-json/overview), [FileStream](https://learn.microsoft.com/en-us/dotnet/api/system.io.filestream)

---

## 11. Wire Protocol Design

### Length-Prefixed Framing
TCP is a byte stream with no built-in message boundaries. We add our own:

```
[ Length:1 ][ Type:1 ][ Payload:Length-1 ]
```

The receiver:
1. Reads 1 byte → `length`
2. Reads exactly `length` bytes → `type + payload`
3. `payload = body[1..]`

```csharp
// Read
var lenBuf = new byte[1];
await ReadExactAsync(stream, lenBuf, ct);
var body = new byte[lenBuf[0]];
await ReadExactAsync(stream, body, ct);
var type = (MessageType)body[0];
var payload = body[1..];
```

### Protocol Design Principles
1. **Fixed-size headers**: Easy to parse without scanning for delimiters
2. **Big-endian integers**: Cross-platform compatibility
3. **Type byte**: Every message self-describes its type
4. **ACK responses**: Synchronous request-response confirms delivery

---

## 12. Key Design Patterns

### Producer-Consumer Pattern
A shared buffer between a producer and consumer thread:
- Producer: `PushBack(item)` under write lock
- Consumer: `PopFront()` / `Peek(offset)` under read lock

### Observer/Fan-Out Pattern
One producer event triggers multiple independent consumers (consumer groups):
- Single ring buffer
- Multiple `Offset` pointers (one per consumer group)
- Each group advances independently

### Reverse TCP Connection Pattern
Used in Kafka and this project: the **server connects back to the client**.

1. Client opens its own listener
2. Client tells server: "my callback port is X"
3. Server connects to client port X
4. Long-lived bidirectional stream established

This avoids the server needing to accept new connections for message delivery.

### Backpressure Pattern
Producer is slowed down when the queue is full:

```csharp
public void PushBack(T item)
{
    if (Count == _capacity)
        throw new InvalidOperationException("Ring buffer is full — apply backpressure.");
    // ...
}
```

The producer catches this exception and slows down or waits.

---

## Learning Resources

### C# and .NET
- [Microsoft Learn C#](https://learn.microsoft.com/en-us/dotnet/csharp/)
- [C# in a Nutshell](https://www.oreilly.com/library/view/c-120-in/9781098147433/) (book)
- [Threading in C#](https://www.albahari.com/threading/) (free online book)

### Networking
- [Beej's Guide to Network Programming](https://beej.us/guide/bgnet/)
- [Computer Networking: A Top-Down Approach](https://gaia.cs.umass.edu/kurose_ross/) (textbook)

### Message Queues / Kafka
- [Kafka: The Definitive Guide](https://www.confluent.io/resources/kafka-the-definitive-guide/)
- [Designing Data-Intensive Applications](https://dataintensive.net/) — Chapter 11 (stream processing)
- [Apache Kafka documentation](https://kafka.apache.org/documentation/)

### Data Structures
- [Circular buffer — Wikipedia](https://en.wikipedia.org/wiki/Circular_buffer)
- [Introduction to Algorithms (CLRS)](https://mitpress.mit.edu/9780262046305/)

### Performance and Benchmarking
- [Pro .NET Memory Management](https://prodotnetmemory.com/) (book)
- [BenchmarkDotNet docs](https://benchmarkdotnet.org/articles/overview.html)
- [.NET Profiling with dotnet-trace](https://learn.microsoft.com/en-us/dotnet/core/diagnostics/dotnet-trace)
