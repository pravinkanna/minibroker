# Technical Specification: "MiniBroker" – A Minimalist Persistent Message Broker

**Author**: Antigravity  
**Status**: Draft  
**Target Language**: Java 21+ (Leveraging Modern Concurrency)

---

## 1. High-Level Architecture

The system is a single-node, persistent message broker. It functions as a centralized log manager that accepts messages from Producers and serves them to Consumers via both Push and Pull logical models.

### Core Components
1.  **Broker Server (Network Layer)**:
    *   Listens on a TCP port.
    *   Uses **Java Virtual Threads (Project Loom)** to handle each client connection. This gives the simplicity of "thread-per-client" with the scalability of non-blocking I/O.
    *   Parses the custom wire protocol and dispatches commands.

2.  **Topic Registry (Domain Layer)**:
    *   Manages the lifecycle of topics (creation, deletion, lookup).
    *   Thread-safe map of `TopicName -> TopicEngine`.

3.  **Topic Engine (Storage & logic)**:
    *   The core unit of concurrency.
    *   Manages the **Persistent Log** on disk.
    *   Maintains a list of active **Push Subscribers**.
    *   Handles read requests for **Pull Consumers**.
    *   Protected by a `ReadWriteLock` (or strict serialization via single-threaded writer approach) to ensure data integrity.

### Data Flow
*   **Producer**: Connects -> Sending `PUBLISH(topic, payload)` -> Broker appends to Disk Log -> ACK to Producer -> (Async) Broadcast to Push Subscribers.
*   **Pull Consumer**: Connects -> Sends `FETCH(topic, offset, batch_size)` -> Broker reads from Disk/Cache -> Returns `messages` -> Consumer processes.
*   **Push Subscriber**: Connects -> Sends `SUBSCRIBE(topic)` -> Connection enters "Listening Mode" -> Broker pushes bytes as they arrive.

---

## 2. Domain Model

### 2.1 Topic
*   A named stream of messages.
*   **Structure**: In this minimal version, a topic is a single partition.
*   **Persistence**: Represented by a directory on disk containing log segments.

### 2.2 Message
*   **ID/Offset**: `long` (64-bit). Monotonically increasing (0, 1, 2...).
*   **Timestamp**: `long` (Unix epoch millis).
*   **Payload**: `byte[]`. Opaque binary data.
*   **Size**: `int`.

### 2.3 Consumer State
*   **Push Consumers**: Ephemeral state. If they disconnect, they lose their place (unless they reconnect and ask to "replay" by switching to Pull or re-subscribing, but standard Push here is "live tail").
*   **Pull Consumers**: Stateful on the *client side*. The client remembers `last_offset` and asks for `last_offset + 1`. The broker is stateless regarding pull consumers.

---

## 3. APIs & Protocols

We will use a **custom binary protocol** over TCP. It is simpler than HTTP for streaming and offers better learning for `java.nio` and byte manipulation.

### 3.1 Wire Protocol Structure
All packets follow this header format:
```
[Length (4 bytes)] [CommandType (1 byte)] [Payload (Variable)]
```
*   `Length`: Total length of the packet (including command byte).
*   `CommandType`: Enum for operation.

### 3.2 Command Types

| Byte | Name | Payload Structure | Response |
| :--- | :--- | :--- | :--- |
| `0x01` | `PUBLISH` | `[TopicLen(2)] [TopicStr] [DataLen(4)] [DataBytes]` | `ACK(Offset)` or `ERR` |
| `0x02` | `SUBSCRIBE` (Push) | `[TopicLen(2)] [TopicStr]` | Stream of `MSG` packets |
| `0x03` | `FETCH` (Pull) | `[TopicLen(2)] [TopicStr] [Offset(8)] [BatchSize(4)]` | `BATCH` packet |
| `0x04` | `CREATE_TOPIC` | `[TopicLen(2)] [TopicStr]` | `OK` or `ERR` |

### 3.3 Response Types
*   **ACK**: `[Cmd=0x81] [Offset(8)]` (Confirm receipt and disk write).
*   **MSG** (Push): `[Cmd=0x82] [Offset(8)] [Timestamp(8)] [DataLen(4)] [Data]`
*   **BATCH** (Pull): `[Cmd=0x83] [Count(4)]` followed by `Count` * `MSG` frames.
*   **ERR**: `[Cmd=0xFF] [Code(2)] [MsgLen(2)] [MsgStr]`

---

## 4. Persistence Design

We focus on the **Append-Only Log** structure.

### 4.1 File Layout
Each topic has a directory: `~/minibroker/data/{topic_name}/`
Inside:
1.  `00000000000000000000.log` (The actual data)
2.  `00000000000000000000.index` (Sparse index mapping Offset -> FilePosition)

**Log File Format**:
A sequence of binary entries:
```
[EntryHeader] [Payload]
```
*   `EntryHeader`: `[CRC32(4)] [Offset(8)] [Timestamp(8)] [Size(4)]`
*   `Payload`: `[Bytes...]`

### 4.2 Indexing (Simplification)
To find "Offset 500" without reading the whole 1GB file:
*   Keep a simple **Sparse Index** in memory (TreeMap) or on disk.
*   Example: Map `Offset -> FilePosition` every 1KB of data or every 100 messages.
*   For this project, we can start with a **dense in-memory ArrayList** of `Long` (file positions) for simplicity, since we aren't targeting petabytes.

### 4.3 Reliability
*   **Flush Policy**: `FileChannel.force(true)` calls.
    *   *Strict Mode*: Flush on every message (Safe but slow).
    *   *Fast Mode*: Flush OS page cache every 500ms or 100 messages.
    *   *Decision*: Configurable. Default to **OS buffer (OS handles flush)** for performance, with a dedicated background thread calling `force()` periodically for durability "checkpoints".

---

## 5. Delivery Semantics & Concurrency

### 5.1 Delivery Semantics
*   **At-Least-Once**:
    *   Producer retries if it doesn't receive `ACK`.
    *   Consumer tracks its own offset. If it crashes and restarts, it processes from the last saved offset.
*   Duplicate detection is out of scope for "minimal", so duplicates are possible during network partitions.

### 5.2 Concurrency Model
**The "Modern Java" Approach (Java 21+)**

*   **Virtual Threads**: We will use `Executors.newVirtualThreadPerTaskExecutor()`.
    *   Each client connection = 1 Virtual Thread.
    *   Blocking I/O operations (`socket.read()`) are cheap. No callback hell (Netty/Node.js style) and no complex `Selector` logic.
    *   This is a massive learning point for modern Java server development.

*   **Synchronization**:
    *   **Writes**: Protected by a `ReentrantLock` per Topic. Only one thread can append to the file at a time to ensure atomic writes.
    *   **Reads**: Can be concurrent. `FileChannel.read(buffer, position)` is thread-safe for random access reads.

---

## 6. Failure Handling

1.  **Broker Crash/Restart**:
    *   On startup, scan topic directories.
    *   "Recover" the last offset by reading the end of the log file.
    *   Truncate any corrupted "half-written" messages at the tail (found via CRC check).

2.  **Consumer Disconnect**:
    *   **Push**: Broker detects `IOException` on write, removes socket from subscriber list.
    *   **Pull**: No issue; connection is transient or idle. Broker does nothing.

3.  **Partial Writes**:
    *   If the server dies while writing a header but not payload, the CRC check on restart will fail. The recovery process truncates the log to the last valid entry.

---

## 7. Trade-offs (What we are NOT doing)

*   **Clustering/Replication**: No Raft/Paxos. Single point of failure.
*   **zero-copy transfer**: We will read bytes into user-space heap memory to parse/send. We won't strictly use `transferTo` (sendfile) everywhere to keep logic observable, though strictly speaking `transferTo` is better for performance.
*   **Complex Compaction**: We won't implement log compaction (key-based retention). Just size-based or time-based retention (delete old segments).

---

## 8. Implementation Roadmap

### Phase 1: The Core Log (No Networking)
*   Implement `LogManager`: `append(byte[])` -> returns offset, `read(offset)` -> returns record.
*   Handle file persistence and basic recovery.
*   **Goal**: Unit tests passing for writing/reading from disk.

### Phase 2: The Wire Protocol
*   Implement `BrokerServer` with Virtual Threads.
*   Build a simple `BrokerClient` (library) for testing.
*   Handle `PUBLISH` command (Network -> Disk).

### Phase 3: Pull Consumption
*   Implement `FETCH` handler.
*   Test Producer -> Broker -> Pull Consumer flow.

### Phase 4: Push Subscriptions & Polish
*   Implement `SUBSCRIBE` handler.
*   Add broadcast logic to the publish path.
*   Add "Recovery/Truncation" logic on startup.

---

## 9. Learning Outcomes (Java Focus)

1.  **Modern Concurrency**: Real-world usage of **Virtual Threads** (Project Loom). You'll see how they replace complex NIO Selectors.
2.  **Java NIO (File)**: Deep dive into `FileChannel`, `ByteBuffer`, and memory-mapped files (optional optimization).
3.  **Binary Protocols**: Handling endianness, framing, and strict byte-level parsing in Java `DataInputStream`/`DataOutputStream`.
4.  **System Design**: Understanding the "stateless broker" vs "stateful broker" trade-offs.

---
