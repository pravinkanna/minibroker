# MiniBroker Builder's Guide: Phase 1 – The Core Log Engine

This is a **deep-dive implementation guide** for building the persistent log engine. By the end, you'll have a fully working, tested, crash-recoverable append-only log.

---

## Table of Contents

1. [Understanding the Log Abstraction](#1-understanding-the-log-abstraction)
2. [Binary Record Format (Byte-Level)](#2-binary-record-format-byte-level)
3. [Implementing LogEntry](#3-implementing-logentry)
4. [Implementing LogSegment (The Core)](#4-implementing-logsegment-the-core)
5. [Implementing TopicLog](#5-implementing-topiclog)
6. [Recovery & Corruption Handling](#6-recovery--corruption-handling)
7. [Unit Testing Strategy](#7-unit-testing-strategy)
8. [Common Pitfalls & Debugging](#8-common-pitfalls--debugging)

---

## 1. Understanding the Log Abstraction

### What is an Append-Only Log?

An append-only log is a file where:
- New data is **always written at the end**
- Existing data is **never modified**
- Each entry has a unique, monotonically increasing **offset** (like an array index)

```
┌─────────┬─────────┬─────────┬─────────┬─────────┐
│ Entry 0 │ Entry 1 │ Entry 2 │ Entry 3 │ Entry 4 │  ← File grows rightward
└─────────┴─────────┴─────────┴─────────┴─────────┘
                                        ↑
                                   Next write here
```

### Why This Design?

| Property | Benefit |
| :--- | :--- |
| Append-only | Sequential writes are fast (no seeking) |
| Immutable entries | No locking for reads |
| Offset-based | Easy to track "where am I" for consumers |
| CRC checksums | Detect corruption after crashes |

### Key Insight: Offset vs File Position

These are **different things**:

- **Offset**: Logical index (0, 1, 2, 3...). This is what consumers use.
- **File Position**: Byte offset in the file (0, 28, 56, 89...). This is where the entry physically starts.

You need a way to translate: `Offset → File Position`. Options:
1. **Dense Index**: Keep an `ArrayList<Long>` where `index.get(offset)` returns file position. Simple, but O(n) memory.
2. **Sparse Index**: Store every Nth offset. Binary search + scan. Less memory.
3. **Fixed-size records**: If all records are same size, `position = offset * recordSize`. Not applicable here (variable payload).

**For V1, use the Dense Index** (ArrayList). It's simple and works for millions of messages.

---

## 2. Binary Record Format (Byte-Level)

Each record on disk looks like this:

```
┌──────────────────────────────────────────────────────────────────┐
│ CRC32 (4) │ Offset (8) │ Timestamp (8) │ Size (4) │ Payload (N) │
└──────────────────────────────────────────────────────────────────┘
     ↑            ↑             ↑             ↑           ↑
   Checksum   Logical ID   Unix millis   Payload len  Actual data
```

### Field Details

| Field | Type | Bytes | Description |
| :--- | :--- | :--- | :--- |
| `CRC32` | `int` | 4 | Checksum of `Offset + Timestamp + Size + Payload` |
| `Offset` | `long` | 8 | Monotonic message ID (0, 1, 2...) |
| `Timestamp` | `long` | 8 | `System.currentTimeMillis()` at write time |
| `Size` | `int` | 4 | Length of payload in bytes |
| `Payload` | `byte[]` | N | The actual message data |

**Header size**: 4 + 8 + 8 + 4 = **24 bytes**
**Total entry size**: 24 + payload.length

### Why CRC32?

If the broker crashes mid-write, you might have:
- A partial header (e.g., only 10 bytes written)
- A complete header but partial payload

On restart, you read each entry and **validate the CRC**. If it doesn't match, you know the entry is corrupted → truncate the file at that point.

### CRC Calculation in Java

```java
import java.util.zip.CRC32;

public long calculateCRC(long offset, long timestamp, int size, byte[] payload) {
    CRC32 crc = new CRC32();
    
    // Feed the fields in order (as bytes)
    ByteBuffer buffer = ByteBuffer.allocate(8 + 8 + 4 + payload.length);
    buffer.putLong(offset);
    buffer.putLong(timestamp);
    buffer.putInt(size);
    buffer.put(payload);
    
    crc.update(buffer.array());
    return crc.getValue(); // Returns long, but we only use lower 32 bits
}
```

---

## 3. Implementing LogEntry

This is the simplest class. It's an immutable data container.

### LogEntry.java

```java
package minibroker.log;

import java.util.Arrays;
import java.util.Objects;

/**
 * Represents a single message in the log.
 * Immutable. Thread-safe by design.
 */
public final class LogEntry {
    
    private final long offset;
    private final long timestamp;
    private final byte[] payload;
    
    public LogEntry(long offset, long timestamp, byte[] payload) {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset cannot be negative: " + offset);
        }
        if (payload == null) {
            throw new NullPointerException("Payload cannot be null");
        }
        this.offset = offset;
        this.timestamp = timestamp;
        // Defensive copy to ensure immutability
        this.payload = Arrays.copyOf(payload, payload.length);
    }
    
    public long offset() {
        return offset;
    }
    
    public long timestamp() {
        return timestamp;
    }
    
    public byte[] payload() {
        // Return a copy to preserve immutability
        return Arrays.copyOf(payload, payload.length);
    }
    
    public int payloadSize() {
        return payload.length;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LogEntry that)) return false;
        return offset == that.offset 
            && timestamp == that.timestamp 
            && Arrays.equals(payload, that.payload);
    }
    
    @Override
    public int hashCode() {
        int result = Objects.hash(offset, timestamp);
        result = 31 * result + Arrays.hashCode(payload);
        return result;
    }
    
    @Override
    public String toString() {
        return "LogEntry{offset=" + offset 
            + ", timestamp=" + timestamp 
            + ", payloadSize=" + payload.length + "}";
    }
}
```

> [!NOTE]
> **Why defensive copies?** If someone passes a `byte[]` and later modifies it, our LogEntry would be corrupted. By copying on construction and on retrieval, we guarantee immutability.

---

## 4. Implementing LogSegment (The Core)

This is the **most important class**. It manages a single `.log` file.

### 4.1 Class Overview

```java
package minibroker.log;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

/**
 * Manages a single log segment file.
 * 
 * Thread-safety:
 * - Writes are serialized via lock
 * - Reads are thread-safe (FileChannel.read with position is atomic)
 */
public class LogSegment implements Closeable {
    
    private static final int HEADER_SIZE = 24; // CRC(4) + Offset(8) + Timestamp(8) + Size(4)
    
    private final Path path;
    private final FileChannel channel;
    private final ReentrantLock writeLock = new ReentrantLock();
    
    // In-memory index: offset -> file position
    private final List<Long> offsetIndex = new ArrayList<>();
    
    private long nextOffset = 0;
    
    public LogSegment(Path path) throws IOException {
        this.path = path;
        this.channel = FileChannel.open(path,
            StandardOpenOption.CREATE,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE
        );
        recover();
    }
    
    // ... methods below
}
```

### 4.2 The `append` Method

```java
/**
 * Append a message to the log.
 * 
 * @param payload The message bytes
 * @return The offset assigned to this message
 * @throws IOException if write fails
 */
public long append(byte[] payload) throws IOException {
    writeLock.lock();
    try {
        long offset = nextOffset;
        long timestamp = System.currentTimeMillis();
        int size = payload.length;
        
        // Calculate CRC
        int crc = calculateCRC(offset, timestamp, size, payload);
        
        // Build the record
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE + size);
        buffer.putInt(crc);
        buffer.putLong(offset);
        buffer.putLong(timestamp);
        buffer.putInt(size);
        buffer.put(payload);
        buffer.flip();
        
        // Record the file position BEFORE writing
        long filePosition = channel.size();
        
        // Write to file
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
        
        // Update in-memory state
        offsetIndex.add(filePosition);
        nextOffset++;
        
        return offset;
        
    } finally {
        writeLock.unlock();
    }
}

private int calculateCRC(long offset, long timestamp, int size, byte[] payload) {
    CRC32 crc = new CRC32();
    ByteBuffer buffer = ByteBuffer.allocate(8 + 8 + 4 + payload.length);
    buffer.putLong(offset);
    buffer.putLong(timestamp);
    buffer.putInt(size);
    buffer.put(payload);
    crc.update(buffer.array());
    return (int) crc.getValue();
}
```

### 4.3 The `read` Method

```java
/**
 * Read an entry by its logical offset.
 * 
 * @param offset The logical offset (0, 1, 2...)
 * @return The LogEntry
 * @throws IOException if read fails
 * @throws IllegalArgumentException if offset doesn't exist
 */
public LogEntry read(long offset) throws IOException {
    if (offset < 0 || offset >= nextOffset) {
        throw new IllegalArgumentException(
            "Offset " + offset + " out of range [0, " + nextOffset + ")"
        );
    }
    
    // Get file position from index
    long filePosition = offsetIndex.get((int) offset);
    
    // Read header
    ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_SIZE);
    int bytesRead = channel.read(headerBuffer, filePosition);
    if (bytesRead < HEADER_SIZE) {
        throw new IOException("Incomplete header at position " + filePosition);
    }
    headerBuffer.flip();
    
    int storedCRC = headerBuffer.getInt();
    long storedOffset = headerBuffer.getLong();
    long timestamp = headerBuffer.getLong();
    int size = headerBuffer.getInt();
    
    // Read payload
    ByteBuffer payloadBuffer = ByteBuffer.allocate(size);
    bytesRead = channel.read(payloadBuffer, filePosition + HEADER_SIZE);
    if (bytesRead < size) {
        throw new IOException("Incomplete payload at position " + filePosition);
    }
    payloadBuffer.flip();
    byte[] payload = new byte[size];
    payloadBuffer.get(payload);
    
    // Validate CRC
    int calculatedCRC = calculateCRC(storedOffset, timestamp, size, payload);
    if (storedCRC != calculatedCRC) {
        throw new IOException("CRC mismatch at offset " + offset);
    }
    
    return new LogEntry(storedOffset, timestamp, payload);
}
```

### 4.4 The `recover` Method

Called on startup to rebuild in-memory state.

```java
/**
 * Scan the file and rebuild the offset index.
 * Truncate any corrupted entries at the end.
 */
private void recover() throws IOException {
    long position = 0;
    long fileSize = channel.size();
    
    while (position + HEADER_SIZE <= fileSize) {
        // Read header
        ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_SIZE);
        int bytesRead = channel.read(headerBuffer, position);
        if (bytesRead < HEADER_SIZE) {
            // Incomplete header - truncate here
            truncateAt(position);
            break;
        }
        headerBuffer.flip();
        
        int storedCRC = headerBuffer.getInt();
        long storedOffset = headerBuffer.getLong();
        long timestamp = headerBuffer.getLong();
        int size = headerBuffer.getInt();
        
        // Sanity check on size
        if (size < 0 || size > 100_000_000) { // Max 100MB message
            truncateAt(position);
            break;
        }
        
        // Check if we have enough bytes for payload
        if (position + HEADER_SIZE + size > fileSize) {
            // Incomplete payload - truncate
            truncateAt(position);
            break;
        }
        
        // Read payload and validate CRC
        ByteBuffer payloadBuffer = ByteBuffer.allocate(size);
        channel.read(payloadBuffer, position + HEADER_SIZE);
        payloadBuffer.flip();
        byte[] payload = new byte[size];
        payloadBuffer.get(payload);
        
        int calculatedCRC = calculateCRC(storedOffset, timestamp, size, payload);
        if (storedCRC != calculatedCRC) {
            // CRC mismatch - truncate
            System.err.println("CRC mismatch at position " + position + ", truncating");
            truncateAt(position);
            break;
        }
        
        // Valid entry - add to index
        offsetIndex.add(position);
        nextOffset = storedOffset + 1;
        position += HEADER_SIZE + size;
    }
    
    System.out.println("Recovered " + nextOffset + " entries from " + path);
}

private void truncateAt(long position) throws IOException {
    System.err.println("Truncating file at position " + position);
    channel.truncate(position);
}
```

### 4.5 Utility Methods

```java
/**
 * Force writes to disk.
 */
public void flush() throws IOException {
    channel.force(true);
}

/**
 * Get the next offset that will be assigned.
 */
public long getNextOffset() {
    return nextOffset;
}

/**
 * Get number of entries.
 */
public long size() {
    return nextOffset;
}

@Override
public void close() throws IOException {
    channel.close();
}
```

---

## 5. Implementing TopicLog

This is a thin wrapper that manages segments for a topic. For V1, we only support a single segment.

### TopicLog.java

```java
package minibroker.log;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.*;

/**
 * Manages the log for a single topic.
 * 
 * Currently supports only a single segment.
 * Future: multiple segments with rollover.
 */
public class TopicLog implements Closeable {
    
    private static final String SEGMENT_FILENAME = "00000000000000000000.log";
    
    private final Path topicDir;
    private final LogSegment activeSegment;
    
    public TopicLog(Path topicDir) throws IOException {
        this.topicDir = topicDir;
        
        // Ensure directory exists
        Files.createDirectories(topicDir);
        
        // Open the segment
        Path segmentPath = topicDir.resolve(SEGMENT_FILENAME);
        this.activeSegment = new LogSegment(segmentPath);
    }
    
    /**
     * Append a message to the topic log.
     * 
     * @param payload Message bytes
     * @return The assigned offset
     */
    public long append(byte[] payload) throws IOException {
        return activeSegment.append(payload);
    }
    
    /**
     * Read a message by offset.
     * 
     * @param offset The logical offset
     * @return The log entry
     */
    public LogEntry read(long offset) throws IOException {
        return activeSegment.read(offset);
    }
    
    /**
     * Get the next offset that will be assigned.
     */
    public long getNextOffset() {
        return activeSegment.getNextOffset();
    }
    
    /**
     * Get the number of messages in the log.
     */
    public long size() {
        return activeSegment.size();
    }
    
    /**
     * Force flush to disk.
     */
    public void flush() throws IOException {
        activeSegment.flush();
    }
    
    @Override
    public void close() throws IOException {
        activeSegment.close();
    }
}
```

---

## 6. Recovery & Corruption Handling

### Crash Scenarios

| Scenario | What Happens | How We Handle |
| :--- | :--- | :--- |
| Crash after writing header, before payload | File has partial record | `recover()` detects incomplete payload, truncates |
| Crash mid-payload | File has partial record | `recover()` detects CRC mismatch or incomplete read, truncates |
| Crash after payload, before index update | Data on disk, but index not updated | `recover()` rebuilds index from file |
| Clean shutdown | Everything synced | No action needed |

### Testing Recovery

```java
@Test
void testRecoveryAfterCorruption() throws Exception {
    Path dir = Files.createTempDirectory("test-topic");
    Path logFile = dir.resolve("00000000000000000000.log");
    
    // Write some entries
    try (TopicLog log = new TopicLog(dir)) {
        log.append("message1".getBytes());
        log.append("message2".getBytes());
        log.append("message3".getBytes());
        log.flush();
    }
    
    // Corrupt the file by truncating last 5 bytes
    try (var raf = new RandomAccessFile(logFile.toFile(), "rw")) {
        raf.setLength(raf.length() - 5);
    }
    
    // Reopen - should recover first 2 entries
    try (TopicLog log = new TopicLog(dir)) {
        assertEquals(2, log.size()); // Third entry was corrupted
        assertEquals("message1", new String(log.read(0).payload()));
        assertEquals("message2", new String(log.read(1).payload()));
    }
}
```

---

## 7. Unit Testing Strategy

### Test Categories

1. **Basic CRUD**
   - Append single entry, read it back
   - Append multiple entries, read all
   - Read non-existent offset → exception

2. **Persistence**
   - Write, close, reopen, read
   - Write many entries, close, reopen, verify all

3. **Recovery**
   - Simulate partial header write
   - Simulate partial payload write
   - Simulate CRC corruption

4. **Concurrency**
   - Multiple threads appending simultaneously
   - Multiple threads reading while one writes

### Sample Test Class

```java
package minibroker.log;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class TopicLogTest {
    
    @TempDir
    Path tempDir;
    
    @Test
    void appendAndReadSingle() throws Exception {
        try (TopicLog log = new TopicLog(tempDir.resolve("test-topic"))) {
            byte[] payload = "Hello, World!".getBytes();
            
            long offset = log.append(payload);
            
            assertEquals(0, offset);
            LogEntry entry = log.read(0);
            assertArrayEquals(payload, entry.payload());
        }
    }
    
    @Test
    void appendAndReadMultiple() throws Exception {
        try (TopicLog log = new TopicLog(tempDir.resolve("test-topic"))) {
            for (int i = 0; i < 100; i++) {
                byte[] payload = ("Message " + i).getBytes();
                long offset = log.append(payload);
                assertEquals(i, offset);
            }
            
            for (int i = 0; i < 100; i++) {
                LogEntry entry = log.read(i);
                assertEquals("Message " + i, new String(entry.payload()));
            }
        }
    }
    
    @Test
    void persistenceAcrossRestart() throws Exception {
        Path topicDir = tempDir.resolve("persist-topic");
        
        // Write
        try (TopicLog log = new TopicLog(topicDir)) {
            log.append("First".getBytes());
            log.append("Second".getBytes());
            log.flush();
        }
        
        // Reopen and read
        try (TopicLog log = new TopicLog(topicDir)) {
            assertEquals(2, log.size());
            assertEquals("First", new String(log.read(0).payload()));
            assertEquals("Second", new String(log.read(1).payload()));
        }
    }
    
    @Test
    void concurrentAppends() throws Exception {
        try (TopicLog log = new TopicLog(tempDir.resolve("concurrent-topic"))) {
            int threadCount = 10;
            int messagesPerThread = 100;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch latch = new CountDownLatch(threadCount);
            
            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    try {
                        for (int i = 0; i < messagesPerThread; i++) {
                            log.append(("T" + threadId + "-M" + i).getBytes());
                        }
                    } catch (Exception e) {
                        fail(e);
                    } finally {
                        latch.countDown();
                    }
                });
            }
            
            latch.await(10, TimeUnit.SECONDS);
            executor.shutdown();
            
            assertEquals(threadCount * messagesPerThread, log.size());
        }
    }
    
    @Test
    void readInvalidOffset() throws Exception {
        try (TopicLog log = new TopicLog(tempDir.resolve("test-topic"))) {
            log.append("test".getBytes());
            
            assertThrows(IllegalArgumentException.class, () -> log.read(1));
            assertThrows(IllegalArgumentException.class, () -> log.read(-1));
        }
    }
}
```

---

## 8. Common Pitfalls & Debugging

### Pitfall 1: ByteBuffer Position

ByteBuffer has a `position` that advances with each read/write. **You must `flip()` before reading from a buffer you just wrote to.**

```java
ByteBuffer buffer = ByteBuffer.allocate(100);
buffer.putInt(42);
buffer.putLong(123L);

// WRONG - position is at 12, nothing to read
channel.write(buffer);

// CORRECT - flip resets position to 0, sets limit to 12
buffer.flip();
channel.write(buffer);
```

### Pitfall 2: FileChannel Position

`FileChannel.write(buffer)` writes at the channel's position and advances it.
`FileChannel.write(buffer, position)` writes at the specified position (doesn't change channel position).

For our append-only log, we use `channel.write(buffer)` and rely on `StandardOpenOption.APPEND`... **but wait**, that option doesn't exist for `FileChannel.open`!

**Solution**: Always seek to end before writing:
```java
// In append() method:
channel.position(channel.size());
channel.write(buffer);
```

Or use `RandomAccessFile` which has explicit `seek()`.

### Pitfall 3: CRC32 Returns Long

`CRC32.getValue()` returns a `long`, but CRC32 is actually 32 bits. Cast to `int`:

```java
int crc = (int) crc32.getValue();
```

### Pitfall 4: Memory Leaks with ByteBuffer

If you allocate `ByteBuffer.allocateDirect()`, it's not GC'd normally. Prefer `ByteBuffer.allocate()` (heap) for simplicity, or reuse direct buffers.

### Debugging Tips

1. **Hex dump the log file**:
   ```bash
   xxd data/my-topic/00000000000000000000.log | head -50
   ```

2. **Add logging**:
   ```java
   System.out.printf("Appending: offset=%d, position=%d, size=%d%n", 
       offset, filePosition, payload.length);
   ```

3. **Write a log inspector tool**:
   ```java
   public static void main(String[] args) throws Exception {
       Path logFile = Path.of(args[0]);
       try (TopicLog log = new TopicLog(logFile.getParent())) {
           for (long i = 0; i < log.size(); i++) {
               LogEntry entry = log.read(i);
               System.out.println(entry);
           }
       }
   }
   ```

---

## Summary

After completing Phase 1, you will have:

- [x] `LogEntry` - Immutable message container
- [x] `LogSegment` - Single file I/O with CRC validation
- [x] `TopicLog` - Topic-level abstraction
- [x] Recovery logic that handles crashes
- [x] Comprehensive unit tests

You're now ready for **Phase 2: The Wire Protocol**!
