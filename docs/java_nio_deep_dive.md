# MiniBroker: Java NIO Deep Dive

This guide covers the **Java NIO** (New I/O) APIs used throughout MiniBroker.

---

## Table of Contents

1. [Why NIO Instead of Classic IO?](#1-why-nio-instead-of-classic-io)
2. [ByteBuffer Mastery](#2-bytebuffer-mastery)
3. [FileChannel Operations](#3-filechannel-operations)
4. [Memory-Mapped Files (Optional)](#4-memory-mapped-files-optional)
5. [Common Patterns in MiniBroker](#5-common-patterns-in-minibroker)
6. [Debugging NIO Code](#6-debugging-nio-code)

---

## 1. Why NIO Instead of Classic IO?

### Classic IO (java.io)

```java
// Simple but limited
FileOutputStream fos = new FileOutputStream("data.log");
fos.write(bytes);
fos.close();
```

**Limitations**:
- No random access (must read sequentially)
- No control over buffering
- Stream-based (one byte/array at a time)

### NIO (java.nio)

```java
// More control, better performance
FileChannel channel = FileChannel.open(path, READ, WRITE, CREATE);
ByteBuffer buffer = ByteBuffer.allocate(1024);
buffer.put(bytes);
buffer.flip();
channel.write(buffer);
```

**Advantages**:
- Random access via positions
- Direct memory buffers (zero-copy potential)
- Explicit control over buffering
- `force()` for durability control

---

## 2. ByteBuffer Mastery

### The Mental Model

A ByteBuffer is like a **sliding window** over an array:

```
                  position         limit        capacity
                     ↓               ↓             ↓
┌───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┐
│ H │ e │ l │ l │ o │   │   │   │   │   │   │   │   │
└───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┘
  0   1   2   3   4   5   6   7   8   9  10  11  12

position = where next read/write happens
limit    = boundary of readable/writable area
capacity = total size of buffer
```

### Key Operations

#### Creating Buffers

```java
// Heap buffer (backed by byte[])
ByteBuffer heap = ByteBuffer.allocate(1024);

// Direct buffer (native memory, faster for I/O)
ByteBuffer direct = ByteBuffer.allocateDirect(1024);

// Wrap existing array
byte[] data = new byte[100];
ByteBuffer wrapped = ByteBuffer.wrap(data);
```

#### Writing to Buffer

```java
ByteBuffer buf = ByteBuffer.allocate(100);

buf.put((byte) 0x01);      // Single byte
buf.putInt(42);            // 4 bytes (big-endian)
buf.putLong(123456L);      // 8 bytes
buf.put(byteArray);        // Byte array

// After writing: position has moved forward
// Before reading/sending: MUST call flip()
```

#### The Critical flip() Operation

```java
ByteBuffer buf = ByteBuffer.allocate(20);
buf.putInt(100);
buf.putInt(200);
// State: position=8, limit=20

// WRONG - writes garbage (positions 8-19)
channel.write(buf);

// CORRECT - flip sets limit=position, position=0
buf.flip();
// State: position=0, limit=8
channel.write(buf); // Writes bytes 0-7
```

**flip() Rule**: Always flip before switching from write mode to read mode.

#### Reading from Buffer

```java
ByteBuffer buf = ByteBuffer.allocate(100);
channel.read(buf);  // Fills buffer, moves position
buf.flip();         // Prepare to read

int value = buf.getInt();    // Reads 4 bytes
long offset = buf.getLong(); // Reads 8 bytes
byte[] payload = new byte[remaining];
buf.get(payload);            // Reads into array
```

#### Clearing and Reusing

```java
// After reading is done, clear for reuse
buf.clear();  // position=0, limit=capacity
// Buffer is now ready for writing again

// Or if you want to re-read same data:
buf.rewind(); // position=0, limit unchanged
```

### Buffer State Diagram

```
             allocate(N)
                 │
                 ▼
    ┌─────────────────────────┐
    │  WRITE MODE             │
    │  position: 0            │
    │  limit: N               │
    └───────────┬─────────────┘
                │ put(...) moves position
                │
                ▼
    ┌─────────────────────────┐
    │  WRITE MODE (mid)       │
    │  position: X            │
    │  limit: N               │
    └───────────┬─────────────┘
                │ flip()
                ▼
    ┌─────────────────────────┐
    │  READ MODE              │
    │  position: 0            │
    │  limit: X               │
    └───────────┬─────────────┘
                │ get(...) moves position
                │
                ▼
    ┌─────────────────────────┐
    │  READ MODE (done)       │
    │  position: X            │
    │  limit: X               │
    └───────────┬─────────────┘
                │ clear()
                ▼
           Back to WRITE MODE
```

---

## 3. FileChannel Operations

### Opening a FileChannel

```java
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import static java.nio.file.StandardOpenOption.*;

// For reading
FileChannel readChannel = FileChannel.open(path, READ);

// For writing (creates if not exists)
FileChannel writeChannel = FileChannel.open(path, WRITE, CREATE);

// For append-only (MiniBroker's pattern)
FileChannel appendChannel = FileChannel.open(path, 
    READ, WRITE, CREATE);
```

### Writing

```java
// Sequential write (at current position)
ByteBuffer buf = ...;
channel.write(buf);  // position advances

// Positional write (at specific location)
channel.write(buf, position);  // channel position unchanged
```

### Reading

```java
// Sequential read
ByteBuffer buf = ByteBuffer.allocate(1024);
int bytesRead = channel.read(buf);

// Positional read (thread-safe for concurrent reads!)
int bytesRead = channel.read(buf, position);
```

### Seeking

```java
// Get current position
long pos = channel.position();

// Set position (for next read/write)
channel.position(100);

// Seek to end (for append)
channel.position(channel.size());
```

### Durability with force()

```java
// Flush OS buffers to physical disk
channel.force(true);   // Flush data AND metadata
channel.force(false);  // Flush data only (slightly faster)
```

**When to use**:
- After every write: Maximum durability, slow
- Periodically: Balance of speed and durability
- Before ACK: Ensures message won't be lost

### Truncation

```java
// Remove data after position 1000
channel.truncate(1000);
// File is now max 1000 bytes
```

**Use case**: Recovery - truncate corrupted tail.

---

## 4. Memory-Mapped Files (Optional)

Memory-mapped files map file contents directly to memory. Reads/writes become memory operations.

```java
// Map 1MB of file starting at position 0
MappedByteBuffer mmap = channel.map(
    FileChannel.MapMode.READ_WRITE,
    0,         // position
    1024*1024  // size
);

// Now access like a ByteBuffer
int value = mmap.getInt(100);  // Read at offset 100
mmap.putInt(200, 42);          // Write at offset 200

// Force changes to disk
mmap.force();
```

**Advantages**:
- OS handles caching efficiently
- Very fast for random access
- No explicit read() calls needed

**Disadvantages**:
- Fixed mapping size
- Harder to handle growing files
- Some JVM memory management quirks

**MiniBroker**: Start without mmap. Add as optimization later.

---

## 5. Common Patterns in MiniBroker

### Pattern: Writing a Record

```java
public long appendRecord(long offset, long timestamp, byte[] payload) {
    int recordSize = 4 + 8 + 8 + 4 + payload.length;
    // [CRC(4)][Offset(8)][Timestamp(8)][Size(4)][Payload]
    
    ByteBuffer buffer = ByteBuffer.allocate(recordSize);
    
    // Calculate CRC of content (not including CRC field itself)
    int crc = calculateCRC(offset, timestamp, payload);
    
    buffer.putInt(crc);
    buffer.putLong(offset);
    buffer.putLong(timestamp);
    buffer.putInt(payload.length);
    buffer.put(payload);
    
    buffer.flip();  // ← CRITICAL!
    
    long filePosition = channel.size();
    while (buffer.hasRemaining()) {
        channel.write(buffer);
    }
    
    return filePosition;
}
```

### Pattern: Reading a Record at Position

```java
public LogEntry readRecord(long filePosition) throws IOException {
    // Read header first
    ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
    int bytesRead = channel.read(header, filePosition);
    if (bytesRead < HEADER_SIZE) {
        throw new IOException("Incomplete header");
    }
    header.flip();  // ← CRITICAL!
    
    int crc = header.getInt();
    long offset = header.getLong();
    long timestamp = header.getLong();
    int size = header.getInt();
    
    // Read payload
    ByteBuffer payloadBuf = ByteBuffer.allocate(size);
    bytesRead = channel.read(payloadBuf, filePosition + HEADER_SIZE);
    if (bytesRead < size) {
        throw new IOException("Incomplete payload");
    }
    payloadBuf.flip();  // ← CRITICAL!
    
    byte[] payload = new byte[size];
    payloadBuf.get(payload);
    
    return new LogEntry(offset, timestamp, payload);
}
```

### Pattern: Scanning File for Recovery

```java
public void recoverLog() throws IOException {
    long position = 0;
    long fileSize = channel.size();
    
    while (position + HEADER_SIZE <= fileSize) {
        ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
        channel.read(header, position);
        header.flip();
        
        int storedCRC = header.getInt();
        long offset = header.getLong();
        long timestamp = header.getLong();
        int size = header.getInt();
        
        // Validate size is reasonable
        if (size < 0 || position + HEADER_SIZE + size > fileSize) {
            truncateAt(position);
            break;
        }
        
        // Read payload and verify CRC
        ByteBuffer payload = ByteBuffer.allocate(size);
        channel.read(payload, position + HEADER_SIZE);
        payload.flip();
        
        byte[] payloadBytes = new byte[size];
        payload.get(payloadBytes);
        
        int calculated = calculateCRC(offset, timestamp, payloadBytes);
        if (calculated != storedCRC) {
            truncateAt(position);
            break;
        }
        
        // Valid record - add to index
        offsetIndex.add(position);
        position += HEADER_SIZE + size;
    }
}
```

---

## 6. Debugging NIO Code

### Common Error: Forgetting flip()

**Symptom**: Writing garbage or zero bytes.

```java
// Bug
ByteBuffer buf = ByteBuffer.allocate(100);
buf.putInt(42);
channel.write(buf);  // Writes positions 4-99 (garbage!)

// Fix
buf.flip();
channel.write(buf);  // Writes positions 0-3 (the int)
```

### Common Error: Not Checking Bytes Read

**Symptom**: Incomplete reads, corrupt data.

```java
// Bug
ByteBuffer buf = ByteBuffer.allocate(100);
channel.read(buf);  // Might read less than 100!
buf.flip();
int value = buf.getInt();  // Might fail or read garbage

// Fix
ByteBuffer buf = ByteBuffer.allocate(100);
while (buf.hasRemaining()) {
    if (channel.read(buf) == -1) {
        throw new EOFException();
    }
}
```

### Common Error: Buffer Endianness

**Symptom**: Numbers look wrong.

```java
// Java ByteBuffer default is BIG_ENDIAN (network order)
// If reading data from little-endian source:
ByteBuffer buf = ByteBuffer.allocate(4);
buf.order(ByteOrder.LITTLE_ENDIAN);
int value = buf.getInt();
```

### Debugging Helper: Hex Dump

```java
public static void hexDump(ByteBuffer buf) {
    buf.mark();
    StringBuilder sb = new StringBuilder();
    while (buf.hasRemaining()) {
        sb.append(String.format("%02X ", buf.get()));
    }
    System.out.println(sb);
    buf.reset();
}
```

### Debugging Helper: Buffer State

```java
public static void printBufferState(ByteBuffer buf) {
    System.out.printf("position=%d, limit=%d, capacity=%d, remaining=%d%n",
        buf.position(), buf.limit(), buf.capacity(), buf.remaining());
}
```

---

## Quick Reference

```
┌─────────────────────────────────────────────────────────────────┐
│                      BYTEBUFFER CHEAT SHEET                     │
├─────────────────────────────────────────────────────────────────┤
│ allocate(n)     Create heap buffer of n bytes                  │
│ allocateDirect  Create off-heap buffer (faster I/O)            │
│ wrap(byte[])    Wrap existing array                            │
├─────────────────────────────────────────────────────────────────┤
│ put(byte)       Write 1 byte, advance position                 │
│ putInt(int)     Write 4 bytes (big-endian)                     │
│ putLong(long)   Write 8 bytes                                  │
│ put(byte[])     Write array                                    │
├─────────────────────────────────────────────────────────────────┤
│ flip()          Switch to read mode (limit=position, pos=0)    │
│ clear()         Switch to write mode (position=0, limit=cap)   │
│ rewind()        Re-read from start (position=0)                │
│ remaining()     Bytes left to read/write (limit - position)    │
├─────────────────────────────────────────────────────────────────┤
│ get()           Read 1 byte, advance position                  │
│ getInt()        Read 4 bytes                                   │
│ getLong()       Read 8 bytes                                   │
│ get(byte[])     Read into array                                │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                      FILECHANNEL CHEAT SHEET                    │
├─────────────────────────────────────────────────────────────────┤
│ open(path, opts)  Open file with options (READ, WRITE, CREATE) │
│ read(buf)         Read into buffer at current position         │
│ read(buf, pos)    Read at specific position (thread-safe!)     │
│ write(buf)        Write from buffer at current position        │
│ write(buf, pos)   Write at specific position                   │
│ position()        Get current position                         │
│ position(n)       Set position                                 │
│ size()            Get file size                                │
│ truncate(n)       Shrink file to n bytes                       │
│ force(true)       Sync to disk (data + metadata)               │
└─────────────────────────────────────────────────────────────────┘
```
