# DotnetBroker — Architecture Overview

## System Diagram

```
┌─────────────────────────────────────────────────────────┐
│                    KAdmin (port 10000)                  │
│                                                         │
│  Admin Listener ──► Topic Registry                      │
│        │                  │                             │
│        │           ┌──────┴──────┐                      │
│        │           │    Topic    │  RingBufferQueue<PCM> │
│        │           │  [topicId]  │  capacity=10,000      │
│        │           └──────┬──────┘                      │
│        │                  │                             │
│        │        ┌─────────┴─────────┐                   │
│        │        │  ConsumerGroup[]  │                   │
│        │        │   offset tracking │                   │
│        │        └─────────┬─────────┘                   │
│        │                  │                             │
│  ProducerHandler    ConsumerGroupAdvancer               │
│  (per producer)     (per consumer group)                │
└─────────────────────────────────────────────────────────┘
         ▲                  │
    TCP PCM             TCP PCM
         │                  ▼
   ┌──────────┐       ┌──────────┐
   │ Producer │       │ Consumer │
   └──────────┘       └──────────┘
```

## Component Responsibilities

### `BrokerServer`
- Listens on admin port (default 10000)
- Handles `ECHO`, `P_REG`, `C_REG` commands
- Creates `Topic` and `ConsumerGroup` objects
- Establishes **reverse TCP connections** to producers and consumers
- Spawns `ProducerHandler` and `ConsumerGroupAdvancer` tasks

### `ProducerHandler`
- Runs as a background `Task` per producer connection
- Reads `PCM` messages from the producer's TCP stream
- Pushes messages into the topic's `RingBufferQueue`
- Sends `R_PCM` ACK after each message

### `ConsumerGroupAdvancer`
- Runs as a background `Task` per consumer group
- Peeks at `queue[group.Offset]` using absolute offset indexing
- **Push mode**: Immediately sends message to next available consumer
- **Pull mode**: Waits for `C_RD` signal from consumer, sends `R_C_RD` ACK, then delivers
- Uses exponential backoff (10ms → 640ms) when no messages are available
- Advances `group.Offset` after successful delivery + ACK

### `RingBufferQueue<T>`
- Fixed-capacity O(1) circular buffer
- `PushBack` / `PopFront` with write-lock
- `Peek(absoluteOffset)` / `TryPeek(absoluteOffset, out item)` with read-lock
- `PopCount` tracks how many items have been popped for absolute offset math
- Multiple consumer groups can peek independently without interfering

### `PersistenceManager`
- **Snapshot**: JSON file (`snapshot.json`) with topic IDs and consumer group IDs
- **Offset Journal**: Binary file per CG (`cg_offset_{id}.bin`) storing current offset
- **Recovery**: On startup, rebuilds topic/CG structures and restores offsets

## Connection Flow

### Producer Registration
```
1. Producer: TcpListener.Start(port=X)
2. Producer→KAdmin: TCP connect to port 10000
3. Producer→KAdmin: P_REG { topic, port=X }
4. KAdmin→Producer: TCP connect to port X (reverse connection)
5. KAdmin→Producer: R_P_REG { ack=0 }  ← sent via callback connection
6. [Bidirectional PCM stream open]
```

### Consumer Registration
```
1. Consumer: TcpListener.Start(port=Y)
2. Consumer→KAdmin: TCP connect to port 10000
3. Consumer→KAdmin: C_REG { topic, port=Y, group_id, mode }
4. KAdmin→Consumer: TCP connect to port Y (reverse connection)
5. KAdmin→Consumer: R_C_REG { ack=0 }  ← sent via callback connection
6. [Message delivery stream open]
```

## Data Flow — Message Delivery

```
Producer → PCM → KAdmin:
  RingBufferQueue.PushBack(message)

KAdmin → Consumer (Push mode):
  loop:
    TryPeek(offset) → if found:
      ConsumerStream.Write(PCM)
      ConsumerStream.Read(R_PCM)
      offset++
    else:
      await Task.Delay(backoff)

KAdmin → Consumer (Pull mode):
  loop:
    TryPeek(offset) → if found:
      ConsumerStream.Read(C_RD)    ← consumer signals ready
      ConsumerStream.Write(R_C_RD)
      ConsumerStream.Write(PCM)
      ConsumerStream.Read(R_PCM)
      offset++
    else:
      await Task.Delay(backoff)
```

## Multi-CG Offset Management

Each consumer group has an independent `Offset` field. Messages in the queue are **never deleted** until all groups have consumed them. A background sweep (future: triggered on offset advance) calls `queue.PopFront()` when `min(all_offsets) > queue.PopCount`.

```
Queue state after 3 messages, 2 CGs:

PopCount=0  Head=0
[MSG_0][MSG_1][MSG_2][...empty...]

CG-100: Offset=2 (consumed MSG_0, MSG_1)
CG-200: Offset=1 (consumed MSG_0 only)

min_offset = 1 → can PopFront() once
After pop: PopCount=1, queue=[MSG_1][MSG_2]
```

## Concurrency Model

| Concern | Mechanism |
|---------|-----------|
| Queue writes (ProducerHandler) | `ReaderWriterLockSlim` write lock |
| Queue reads (ConsumerGroupAdvancer) | `ReaderWriterLockSlim` read lock |
| Consumer list mutations | `SemaphoreSlim(1,1)` |
| Topic/group mutations | `SemaphoreSlim(1,1)` |
| Topic dictionary | `ConcurrentDictionary<uint, Topic>` |
| Offset increment | `Interlocked.Increment` |
