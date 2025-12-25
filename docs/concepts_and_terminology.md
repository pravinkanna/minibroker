# MiniBroker: Core Concepts & Terminology

This document explains the fundamental concepts you'll encounter while building MiniBroker.

---

## Table of Contents

1. [Message Broker Fundamentals](#1-message-broker-fundamentals)
2. [MiniBroker Domain Model](#2-minibroker-domain-model)
3. [Delivery Semantics Explained](#3-delivery-semantics-explained)
4. [Push vs Pull Consumption](#4-push-vs-pull-consumption)
5. [Offset Management](#5-offset-management)
6. [Persistence Concepts](#6-persistence-concepts)

---

## 1. Message Broker Fundamentals

### What is a Message Broker?

A message broker is middleware that:
- **Receives** messages from producers
- **Stores** them durably
- **Delivers** them to consumers

```
┌──────────┐     ┌─────────────┐     ┌──────────┐
│ Producer │────▶│   BROKER    │────▶│ Consumer │
└──────────┘     │  (Storage)  │     └──────────┘
                 └─────────────┘
```

### Why Use a Message Broker?

| Problem | Broker Solution |
| :--- | :--- |
| Producer/Consumer speed mismatch | Broker buffers messages |
| Consumer temporarily offline | Messages wait in broker |
| Multiple consumers need same data | Broker can fan-out |
| Need message history/replay | Broker persists messages |

### Key Terms

| Term | Definition |
| :--- | :--- |
| **Producer** | Application that sends messages to the broker |
| **Consumer** | Application that receives messages from the broker |
| **Topic** | Named category/channel for messages |
| **Message** | Unit of data (payload + metadata) |
| **Offset** | Unique position identifier for a message |
| **Broker** | The server that manages topics and messages |

---

## 2. MiniBroker Domain Model

### Topic

A **Topic** is a named stream of messages. Think of it as a named log file.

```
Topic: "user-signups"
┌─────────┬─────────┬─────────┬─────────┬─────────┐
│ Msg 0   │ Msg 1   │ Msg 2   │ Msg 3   │ Msg 4   │
│ Alice   │ Bob     │ Carol   │ Dave    │ Eve     │
└─────────┴─────────┴─────────┴─────────┴─────────┘
```

**Properties**:
- Messages are ordered within a topic
- Messages are immutable once written
- Topics are independent of each other

### Message

A **Message** consists of:

```java
record Message(
    long offset,      // Position in topic (0, 1, 2...)
    long timestamp,   // When it was received
    byte[] payload    // The actual data
)
```

**Key Points**:
- Payload is opaque bytes (broker doesn't interpret it)
- Offset is assigned by broker, not producer
- Timestamp is broker receive time

### Offset

An **Offset** is the unique identifier for a message's position.

```
Offset:   0       1       2       3       4
        ┌───────┬───────┬───────┬───────┬───────┐
Data:   │ Hello │ World │ Foo   │ Bar   │ Baz   │
        └───────┴───────┴───────┴───────┴───────┘
```

**Why Offsets Matter**:
- Consumer can start reading from any offset
- Consumer tracks "where am I" via offset
- Enables replay: "read from offset 0" = read all history

---

## 3. Delivery Semantics Explained

### The Three Guarantees

| Semantic | Meaning | Trade-off |
| :--- | :--- | :--- |
| **At-most-once** | Message delivered 0 or 1 times | May lose messages |
| **At-least-once** | Message delivered 1 or more times | May have duplicates |
| **Exactly-once** | Message delivered exactly 1 time | Complex, expensive |

### MiniBroker Uses: At-Least-Once

**How it works**:

1. Producer sends message
2. Broker writes to disk
3. Broker sends ACK to producer
4. If producer doesn't get ACK, it retries

**Potential Duplicate Scenario**:
```
Producer ──PUBLISH──▶ Broker (writes to disk)
         ◀──ACK────── Broker
              ❌ Network drops ACK
Producer ──PUBLISH──▶ Broker (writes AGAIN!)
         ◀──ACK────── Broker ✓
```

**Result**: Same message written twice. Consumer sees duplicate.

**Why This is OK**:
- Simpler implementation
- Consumer can deduplicate if needed (using message ID in payload)
- Better than losing messages

---

## 4. Push vs Pull Consumption

### Pull-Based Consumption

Consumer **asks** for messages.

```
Consumer: "Give me messages 0-99"
Broker:   "Here are messages 0-99"
Consumer: (processes them)
Consumer: "Give me messages 100-199"
...
```

**Advantages**:
- Consumer controls pace
- Consumer handles backpressure naturally
- Simpler broker (stateless regarding consumer position)

**Disadvantages**:
- Higher latency (polling interval)
- Wasted requests if no new messages

### Push-Based Consumption

Broker **sends** messages as they arrive.

```
Consumer: "Subscribe to topic X"
Broker:   (connection stays open)
          ──MSG 0──▶
          ──MSG 1──▶
          ──MSG 2──▶
          ...
```

**Advantages**:
- Low latency (immediate delivery)
- Efficient (no empty polls)

**Disadvantages**:
- Backpressure is tricky (what if consumer is slow?)
- Consumer must be online

### MiniBroker: Supports Both!

- **FETCH** command = Pull
- **SUBSCRIBE** command = Push

---

## 5. Offset Management

### Who Tracks the Offset?

**Option A: Broker-managed** (like Kafka consumer groups)
- Broker stores "consumer X is at offset Y"
- Pro: Consumer can crash and resume
- Con: Broker is stateful, complex

**Option B: Consumer-managed** (MiniBroker's approach)
- Consumer remembers its own offset
- Consumer saves offset to its own storage
- Pro: Simple broker
- Con: Consumer must handle persistence

### Consumer Offset Workflow

```
1. Consumer starts, reads saved offset (e.g., 42)
2. Consumer sends FETCH(offset=42, count=100)
3. Broker returns messages 42-141
4. Consumer processes messages
5. Consumer saves new offset (142) to local file/DB
6. Repeat from step 2
```

### Handling Consumer Restart

```java
// On startup
long lastOffset = readOffsetFromFile(); // e.g., 42

// After processing batch
void commitOffset(long newOffset) {
    writeOffsetToFile(newOffset);
}
```

---

## 6. Persistence Concepts

### Append-Only Log

Messages are **only appended**, never modified or deleted (until retention kicks in).

```
Time T1: [Msg0]
Time T2: [Msg0][Msg1]
Time T3: [Msg0][Msg1][Msg2]
```

**Why Append-Only?**
- Sequential writes are FAST (10-100x vs random writes)
- No fragmentation
- Simple recovery (just scan from start)

### Write-Ahead Logging (WAL)

Data is written to disk **before** acknowledging the producer.

```
1. Receive message
2. Write to disk file   ← This MUST happen
3. fsync (optional)     ← Ensure it's really on disk
4. Send ACK to producer
```

### fsync and Durability

```java
// Data might be in OS buffer, not on physical disk
channel.write(buffer);

// Forces data to physical disk (slow but safe)
channel.force(true);
```

**Trade-off**:
- `force(true)` on every write = very safe, very slow
- Periodic `force()` = faster, but may lose recent data on crash

### Segment Files

Large topics are split into segments for manageability:

```
topic-orders/
├── 00000000000000000000.log   (offsets 0-999999)
├── 00000000000001000000.log   (offsets 1000000-1999999)
└── 00000000000002000000.log   (offsets 2000000-...)
```

**Benefits**:
- Can delete old segments (retention)
- Smaller files are easier to handle
- Parallel reads possible

**MiniBroker V1**: Single segment only (simplification).

---

## Quick Reference Card

```
┌─────────────────────────────────────────────────────────────┐
│                    MINIBROKER CONCEPTS                      │
├─────────────────────────────────────────────────────────────┤
│ Topic     = Named message stream                            │
│ Message   = offset + timestamp + payload                    │
│ Offset    = Message position (0, 1, 2...)                   │
│ Producer  = Sends messages (PUBLISH)                        │
│ Consumer  = Receives messages (FETCH or SUBSCRIBE)          │
├─────────────────────────────────────────────────────────────┤
│ FETCH     = Pull N messages starting at offset X            │
│ SUBSCRIBE = Push messages as they arrive                    │
├─────────────────────────────────────────────────────────────┤
│ At-least-once = May have duplicates, won't lose messages    │
│ Append-only   = New data always at end of file              │
│ fsync         = Force data to physical disk                 │
└─────────────────────────────────────────────────────────────┘
```
