# MiniBroker: Architecture Decision Records (ADR)

This document explains the **why** behind key design decisions.

---

## ADR-001: Single-Node Architecture

### Status
Accepted

### Context
Message brokers like Kafka are designed for distributed, multi-node deployment. This adds complexity (consensus protocols, replication, leader election).

### Decision
MiniBroker is single-node only. No clustering, no replication.

### Consequences

**Positive**:
- Dramatically simpler implementation
- No need for consensus (Raft/Paxos)
- Easier to understand and debug
- Faster development

**Negative**:
- Single point of failure
- No horizontal scaling
- Data stored on one machine only

### Rationale
This is a **learning project** focused on understanding message broker internals. Distributed systems complexity would obscure the core concepts (log storage, protocols, consumer models).

---

## ADR-002: Virtual Threads for Concurrency

### Status
Accepted

### Context
Traditional approaches for handling many concurrent connections:
1. **Thread-per-client**: Simple code, but OS threads are expensive (1MB stack each)
2. **NIO with Selectors**: Scalable, but complex callback-based code
3. **Frameworks (Netty)**: Abstracts NIO, but adds dependency

### Decision
Use Java 21's **Virtual Threads** (Project Loom).

### Consequences

**Positive**:
- Simple blocking code (like thread-per-client)
- Scales to millions of connections
- No external dependencies
- Excellent learning opportunity for modern Java

**Negative**:
- Requires Java 21+
- Relatively new feature (less community examples)
- Some libraries may not be virtual-thread-friendly

### Code Example
```java
ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
while (running) {
    Socket client = serverSocket.accept();
    executor.submit(() -> handleClient(client)); // Each in virtual thread
}
```

---

## ADR-003: Custom Binary Protocol

### Status
Accepted

### Context
Options for client-broker communication:
1. **HTTP/REST**: Familiar, tooling support, but overhead for streaming
2. **gRPC**: Efficient, schema-driven, but adds dependency
3. **Custom binary**: Full control, minimal overhead, educational

### Decision
Use a custom length-prefixed binary protocol.

### Format
```
[Length (4 bytes)][Command (1 byte)][Payload (variable)]
```

### Consequences

**Positive**:
- Maximum control over wire format
- Minimal overhead
- Teaches byte-level programming
- Easy to implement streaming (push)

**Negative**:
- No tooling (no `curl` for debugging)
- Must build client libraries from scratch
- Documentation burden

### Rationale
Building a wire protocol from scratch teaches:
- Framing and message boundaries
- Endianness
- Binary serialization
- TCP stream semantics

---

## ADR-004: Append-Only Log Storage

### Status
Accepted

### Context
Storage options:
1. **Database (SQL/NoSQL)**: Powerful, but heavyweight
2. **Key-value store (RocksDB)**: Fast, but external dependency
3. **Custom append-only log**: Simple, fast sequential writes

### Decision
Custom append-only log files.

### Format
```
[CRC32 (4)][Offset (8)][Timestamp (8)][Size (4)][Payload (N)]
```

### Consequences

**Positive**:
- Sequential writes are very fast
- Simple file format
- Easy recovery (scan from start)
- No external dependencies

**Negative**:
- Random reads require index
- Compaction not implemented (unbounded growth)
- Single-file lock contention

### Rationale
This is how Kafka stores data. Understanding this pattern is key to understanding high-performance message brokers.

---

## ADR-005: At-Least-Once Delivery

### Status
Accepted

### Context
Delivery semantics options:
1. **At-most-once**: Fire and forget. May lose messages.
2. **At-least-once**: Retry on failure. May duplicate.
3. **Exactly-once**: Complex. Requires transactions.

### Decision
Implement **at-least-once** delivery.

### Implementation
1. Broker writes message to disk before ACK
2. Producer retries if no ACK received
3. Consumer tracks offset, replays on crash

### Consequences

**Positive**:
- Messages won't be lost (if broker is healthy)
- Simple implementation
- Reasonable for most use cases

**Negative**:
- Duplicates possible (consumer must be idempotent)
- Producer retry logic needed

### Rationale
At-least-once is the sweet spot for learning:
- More interesting than at-most-once
- Less complex than exactly-once
- Demonstrates real-world trade-offs

---

## ADR-006: Consumer-Managed Offsets

### Status
Accepted

### Context
Who tracks consumer progress?
1. **Broker-managed**: Broker stores "consumer X is at offset Y"
2. **Consumer-managed**: Consumer stores its own offset

### Decision
Consumer-managed offsets. Broker is stateless regarding consumer position.

### Consequences

**Positive**:
- Simpler broker
- No consumer group coordination needed
- Consumer has full control over replay

**Negative**:
- Consumer must persist offsets
- No automatic consumer group balancing
- Consumer crash = must restore from saved offset

### Code Example
```java
// Consumer side
long offset = loadFromFile("consumer.offset");
while (true) {
    List<Message> msgs = fetch(topic, offset, 100);
    for (Message m : msgs) {
        process(m);
        offset = m.offset() + 1;
    }
    saveToFile("consumer.offset", offset);
}
```

---

## ADR-007: Single Segment per Topic (V1)

### Status
Accepted (for V1)

### Context
Kafka uses multiple segment files per partition:
```
00000000000000000000.log (messages 0-999999)
00000000000001000000.log (messages 1000000-1999999)
```

### Decision
For V1, use a single segment file per topic.

### Consequences

**Positive**:
- Simpler implementation
- No segment rollover logic
- No segment selection for reads

**Negative**:
- Cannot delete old messages (no segment deletion)
- Large files harder to manage
- Backup/recovery is all-or-nothing

### Future Work
V2 could add:
- Segment rollover at size threshold
- Time-based retention (delete old segments)
- Log compaction

---

## ADR-008: Dense In-Memory Offset Index

### Status
Accepted

### Context
To read message at offset N, we need to find its file position. Options:
1. **Linear scan**: Read all messages until offset N (slow)
2. **Sparse index**: Store every 1000th offset->position (balanced)
3. **Dense index**: Store every offset->position (fast, memory-heavy)

### Decision
Use dense in-memory index (`ArrayList<Long>`).

### Implementation
```java
List<Long> offsetIndex = new ArrayList<>();
// offsetIndex.get(offset) = file position

// On append
long position = channel.size();
offsetIndex.add(position);

// On read
long position = offsetIndex.get((int) offset);
channel.read(buffer, position);
```

### Consequences

**Positive**:
- O(1) offset->position lookup
- Simple implementation
- Fast reads

**Negative**:
- 8 bytes per message (8GB for 1 billion messages)
- Must rebuild on startup

### Rationale
For a learning project with millions (not billions) of messages, this is acceptable. Production systems would use sparse indexes or memory-mapped index files.

---

## ADR-009: CRC32 for Integrity

### Status
Accepted

### Context
Need to detect data corruption. Options:
1. **No checksum**: Fastest, but silent corruption
2. **CRC32**: Fast, good collision resistance
3. **SHA-256**: Cryptographic, slower

### Decision
Use CRC32 checksum for each message.

### Implementation
```java
CRC32 crc = new CRC32();
crc.update(offsetBytes);
crc.update(timestampBytes);
crc.update(sizeBytes);
crc.update(payload);
int checksum = (int) crc.getValue();
```

### Consequences

**Positive**:
- Detects accidental corruption
- Built into Java standard library
- Fast (hardware-accelerated on many CPUs)

**Negative**:
- Not cryptographically secure (can be forged)
- 4 bytes overhead per message
- Computation cost (minimal)

---

## ADR-010: No External Dependencies

### Status
Accepted

### Context
Could use:
- Netty for networking
- RocksDB for storage
- Protobuf for serialization
- SLF4J for logging

### Decision
Use only Java standard library.

### Consequences

**Positive**:
- Maximum learning value
- No dependency management
- Understand everything from bottom up
- Easier to understand for readers

**Negative**:
- Reinventing the wheel
- Less battle-tested code
- More code to write

### Rationale
The goal is **learning**, not building production software. Understanding how these tools work internally is more valuable than using them as black boxes.

---

## Summary Table

| Decision | Choice | Rationale |
| :--- | :--- | :--- |
| Architecture | Single-node | Simplicity, learning focus |
| Concurrency | Virtual Threads | Modern Java, simple + scalable |
| Protocol | Custom binary | Educational, streaming-friendly |
| Storage | Append-only log | Fast, simple, Kafka-like |
| Delivery | At-least-once | Balance of complexity |
| Offset tracking | Consumer-managed | Simpler broker |
| Segments | Single per topic | V1 simplicity |
| Index | Dense in-memory | Fast, acceptable memory |
| Checksum | CRC32 | Fast, built-in |
| Dependencies | None | Maximum learning |
