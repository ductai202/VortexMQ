# Getting Started with DotnetBroker

## Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| .NET SDK | 8.0+ | `dotnet --version` to verify |
| OS | Windows / Linux / macOS | Cross-platform |
| Ports | 10000, 10001, 10002 | Used by default |

## Clone and Build

```bash
# Clone the repository
git clone <repo-url>
cd "message broker/DotnetBroker"

# Build all projects
dotnet build DotnetBroker.sln
```

Expected output:
```
Build succeeded.
    0 Warning(s)
    0 Error(s)
```

## Run the Broker

Open a terminal and start the KAdmin server:

```bash
dotnet run --project src/DotnetBroker.Server
```

Default behavior:
- Listens on port **10000** for admin commands
- Stores persistence data in `./broker_data/`

With custom options:
```bash
dotnet run --project src/DotnetBroker.Server -- --port 10000 --data /path/to/data
```

Expected output:
```
[DotnetBroker.Server] KAdmin starting on port 10000, data dir: broker_data
[Persistence] No snapshot found — starting fresh.
[KAdmin] Listening on port 10000
```

## Start a Consumer

Open a second terminal:

```bash
# Push mode consumer on port 10002, topic 1, group 100
dotnet run --project src/DotnetBroker.Consumer -- 10002 1 100 push

# Pull mode consumer on port 10003, topic 1, group 100
dotnet run --project src/DotnetBroker.Consumer -- 10003 1 100 pull
```

Arguments: `<port> <topic> <group_id> [push|pull] [adminHost] [adminPort]`

Expected output:
```
[Consumer] port=10002 topic=1 group=100 mode=Push admin=127.0.0.1:10000
[Consumer] Listening for KAdmin callback on port 10002
[Consumer] Connected to KAdmin at 127.0.0.1:10000
[Consumer] Sent C_REG: topic=1 group=100 port=10002 mode=Push
[Consumer] Registered successfully with KAdmin.
[Consumer] Receiving messages (mode=Push)...
```

## Start a Producer

Open a third terminal:

```bash
# Producer on port 10001, publishing to topic 1
dotnet run --project src/DotnetBroker.Producer -- 10001 1
```

Arguments: `<port> <topic> [adminHost] [adminPort]`

Expected output:
```
[Producer] port=10001 topic=1 admin=127.0.0.1:10000
[Producer] Listening for KAdmin callback on port 10001
[Producer] Connected to KAdmin at 127.0.0.1:10000
[Producer] Sent P_REG: topic=1 port=10001
[Producer] Registered successfully with KAdmin.
[Producer] Ready. Type messages and press Enter. Ctrl+C to quit.
```

Type a message and press Enter:
```
Hello, DotnetBroker!
[Producer] Sent: Hello, DotnetBroker!
```

The consumer terminal should immediately show:
```
[Consumer-100] Received: "Hello, DotnetBroker!" (ts=1234567890123 from port=10001)
```

## Multi-Consumer-Group Demo

Run 2 consumers in **different groups** for the same topic:

```bash
# Terminal 2: Consumer in Group 100
dotnet run --project src/DotnetBroker.Consumer -- 10002 1 100

# Terminal 3: Consumer in Group 101  
dotnet run --project src/DotnetBroker.Consumer -- 10003 1 101
```

Both groups receive every message independently — this demonstrates **independent offset tracking**.

## Run Tests

### Unit Tests (fast, no network)
```bash
dotnet test tests/DotnetBroker.UnitTests --no-build
```
Expected: 23 tests, all passing.

### Integration Tests (spins up real servers)
```bash
dotnet test tests/DotnetBroker.IntegrationTests --no-build
```
Expected: 7 tests, all passing.

### All Tests
```bash
dotnet test DotnetBroker.sln --no-build
```
Expected: 30 tests, all passing.

## Run Benchmarks

> **Note**: Benchmarks must run in Release mode.

```bash
dotnet run -c Release --project tests/DotnetBroker.Benchmarks
```

This runs:
- `RingBufferBenchmark` — raw push/pop/peek throughput
- `LatencyBenchmark` — payload encode/decode throughput

## Persistence and Recovery

The broker auto-snapshots every 30 seconds and on graceful shutdown. To test recovery:

```bash
# Start broker + consumer + producer, send some messages
# Press Ctrl+C on the broker
# Restart the broker — it will restore topic/CG state from snapshot
dotnet run --project src/DotnetBroker.Server
# Consumers/producers need to re-register (they have no reconnect logic yet)
```

Persistence files in `broker_data/`:
- `snapshot.json` — topic + CG IDs and modes
- `cg_offset_{id}.bin` — last consumed offset per consumer group

## Troubleshooting

| Problem | Solution |
|---------|----------|
| Port already in use | Change port with `-- <port> <topic>` args |
| Consumer gets no messages | Ensure consumer registers BEFORE producer sends |
| Build fails | Run `dotnet restore` then `dotnet build` |
| Tests timeout | Integration tests need 15–25 seconds each; this is expected |
