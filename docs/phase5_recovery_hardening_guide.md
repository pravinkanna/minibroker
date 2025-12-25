# MiniBroker Builder's Guide: Phase 5 – Recovery & Production Hardening

This guide covers making the broker **crash-resistant** and production-ready.

---

## 1. Recovery Overview

When the broker restarts after a crash, it must:

1. **Scan existing topic directories** → Rebuild TopicRegistry
2. **For each topic log** → Validate and recover entries
3. **Truncate corrupted data** → Remove partial writes
4. **Resume normal operation** → Accept new connections

---

## 2. Startup Recovery Flow

```java
package minibroker.topic;

import java.io.IOException;
import java.nio.file.*;
import java.util.stream.Stream;

public class TopicRegistry {
    
    private final Path dataDir;
    private final ConcurrentHashMap<String, Topic> topics = new ConcurrentHashMap<>();
    
    public TopicRegistry(Path dataDir) throws IOException {
        this.dataDir = dataDir;
        Files.createDirectories(dataDir);
        recoverExistingTopics();
    }
    
    private void recoverExistingTopics() throws IOException {
        if (!Files.exists(dataDir)) return;
        
        try (Stream<Path> dirs = Files.list(dataDir)) {
            dirs.filter(Files::isDirectory)
                .forEach(topicDir -> {
                    String name = topicDir.getFileName().toString();
                    try {
                        System.out.println("Recovering topic: " + name);
                        TopicLog log = new TopicLog(topicDir);
                        topics.put(name, new Topic(name, log));
                        System.out.println("  Recovered " + log.size() + " messages");
                    } catch (IOException e) {
                        System.err.println("Failed to recover topic " + name + ": " + e);
                    }
                });
        }
    }
    
    // ... rest of the class
}
```

---

## 3. Log Segment Recovery (Detailed)

The `recover()` method in `LogSegment` does the heavy lifting:

```java
private void recover() throws IOException {
    long position = 0;
    long fileSize = channel.size();
    int validEntries = 0;
    int corruptedEntries = 0;
    
    System.out.println("Scanning log file: " + path);
    System.out.println("  File size: " + fileSize + " bytes");
    
    while (position + HEADER_SIZE <= fileSize) {
        // Read header at position
        ByteBuffer headerBuf = ByteBuffer.allocate(HEADER_SIZE);
        int bytesRead = channel.read(headerBuf, position);
        
        if (bytesRead < HEADER_SIZE) {
            System.err.println("  Incomplete header at " + position);
            truncateAt(position);
            corruptedEntries++;
            break;
        }
        
        headerBuf.flip();
        int storedCRC = headerBuf.getInt();
        long offset = headerBuf.getLong();
        long timestamp = headerBuf.getLong();
        int size = headerBuf.getInt();
        
        // Sanity checks
        if (size < 0) {
            System.err.println("  Negative size at " + position);
            truncateAt(position);
            corruptedEntries++;
            break;
        }
        
        if (size > MAX_MESSAGE_SIZE) {
            System.err.println("  Size exceeds max at " + position);
            truncateAt(position);
            corruptedEntries++;
            break;
        }
        
        // Check payload availability
        long entryEnd = position + HEADER_SIZE + size;
        if (entryEnd > fileSize) {
            System.err.println("  Incomplete payload at " + position);
            truncateAt(position);
            corruptedEntries++;
            break;
        }
        
        // Read and validate payload
        ByteBuffer payloadBuf = ByteBuffer.allocate(size);
        channel.read(payloadBuf, position + HEADER_SIZE);
        payloadBuf.flip();
        byte[] payload = new byte[size];
        payloadBuf.get(payload);
        
        int calculatedCRC = calculateCRC(offset, timestamp, size, payload);
        if (storedCRC != calculatedCRC) {
            System.err.println("  CRC mismatch at " + position);
            System.err.println("    Expected: " + storedCRC + ", Got: " + calculatedCRC);
            truncateAt(position);
            corruptedEntries++;
            break;
        }
        
        // Valid entry!
        offsetIndex.add(position);
        nextOffset = offset + 1;
        position = entryEnd;
        validEntries++;
    }
    
    System.out.println("  Valid entries: " + validEntries);
    if (corruptedEntries > 0) {
        System.out.println("  Truncated corrupted entries: " + corruptedEntries);
    }
}

private void truncateAt(long position) throws IOException {
    long oldSize = channel.size();
    channel.truncate(position);
    System.out.println("  Truncated from " + oldSize + " to " + position);
}
```

---

## 4. Graceful Shutdown

### Shutdown Hook

```java
public class Main {
    
    public static void main(String[] args) throws Exception {
        int port = 9092;
        Path dataDir = Path.of("./data");
        
        TopicRegistry registry = new TopicRegistry(dataDir);
        BrokerServer server = new BrokerServer(port, registry);
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutdown signal received...");
            
            // 1. Stop accepting new connections
            server.stop();
            System.out.println("Stopped accepting connections");
            
            // 2. Flush all logs
            try {
                registry.flushAll();
                System.out.println("Flushed all logs");
            } catch (IOException e) {
                System.err.println("Error flushing: " + e);
            }
            
            // 3. Close all resources
            try {
                registry.closeAll();
                System.out.println("Closed all resources");
            } catch (IOException e) {
                System.err.println("Error closing: " + e);
            }
            
            System.out.println("Shutdown complete");
        }));
        
        server.start();
    }
}
```

### Registry Flush/Close Methods

```java
public class TopicRegistry {
    
    public void flushAll() throws IOException {
        for (Topic topic : topics.values()) {
            topic.flush();
        }
    }
    
    public void closeAll() throws IOException {
        for (Topic topic : topics.values()) {
            topic.close();
        }
        topics.clear();
    }
}

public class Topic {
    
    public void flush() throws IOException {
        log.flush();
    }
}
```

---

## 5. Flush Strategies

### Option 1: Flush Every Write (Safe but Slow)

```java
public long append(byte[] data) throws IOException {
    writeLock.lock();
    try {
        // ... write data ...
        channel.force(true); // Sync to disk
        return offset;
    } finally {
        writeLock.unlock();
    }
}
```

**Latency**: 1-10ms per write (depends on disk)

### Option 2: Background Flusher (Fast, Slightly Less Safe)

```java
public class LogSegment {
    
    private final ScheduledExecutorService flusher;
    
    public LogSegment(Path path) throws IOException {
        // ... existing init ...
        
        flusher = Executors.newSingleThreadScheduledExecutor();
        flusher.scheduleAtFixedRate(this::backgroundFlush, 
            500, 500, TimeUnit.MILLISECONDS);
    }
    
    private void backgroundFlush() {
        try {
            channel.force(false); // Flush data, not metadata
        } catch (IOException e) {
            System.err.println("Background flush failed: " + e);
        }
    }
    
    @Override
    public void close() throws IOException {
        flusher.shutdown();
        channel.force(true);
        channel.close();
    }
}
```

**Trade-off**: Up to 500ms of data loss if crash occurs.

### Option 3: Configurable (Production Choice)

```java
public enum FlushPolicy {
    EVERY_WRITE,      // Safest, slowest
    PERIODIC_500MS,   // Balanced
    OS_DEFAULT        // Fastest, least safe
}
```

---

## 6. Error Handling Best Practices

### Don't Swallow Exceptions

```java
// BAD
try {
    topic.publish(data);
} catch (IOException e) {
    // Silently ignored!
}

// GOOD
try {
    topic.publish(data);
} catch (IOException e) {
    writer.writeError(0x0004, "Publish failed: " + e.getMessage());
    throw e; // Or log and handle appropriately
}
```

### Use Try-with-Resources

```java
// BAD - resource leak if exception thrown
FileChannel channel = FileChannel.open(path);
channel.write(buffer);
channel.close();

// GOOD - always closes
try (FileChannel channel = FileChannel.open(path)) {
    channel.write(buffer);
}
```

### Handle InterruptedException Properly

```java
// BAD - loses interrupt status
try {
    Thread.sleep(1000);
} catch (InterruptedException e) {
    // Ignored
}

// GOOD - restore interrupt flag
try {
    Thread.sleep(1000);
} catch (InterruptedException e) {
    Thread.currentThread().interrupt();
    return; // Exit the loop/method
}
```

---

## 7. Testing Recovery

### Test: Crash During Write

```java
@Test
void testRecoveryAfterPartialWrite() throws Exception {
    Path dir = tempDir.resolve("crash-test");
    Path logFile = dir.resolve("00000000000000000000.log");
    
    // Write valid entries
    try (TopicLog log = new TopicLog(dir)) {
        log.append("Message 1".getBytes());
        log.append("Message 2".getBytes());
        log.flush();
    }
    
    // Simulate crash: append garbage bytes
    try (var out = new FileOutputStream(logFile.toFile(), true)) {
        out.write(new byte[] {0x00, 0x01, 0x02, 0x03, 0x04});
    }
    
    // Recover
    try (TopicLog log = new TopicLog(dir)) {
        assertEquals(2, log.size()); // Only valid entries
    }
}
```

### Test: Corrupt CRC

```java
@Test
void testRecoveryCorruptCRC() throws Exception {
    Path dir = tempDir.resolve("crc-test");
    Path logFile = dir.resolve("00000000000000000000.log");
    
    try (TopicLog log = new TopicLog(dir)) {
        log.append("Message".getBytes());
        log.flush();
    }
    
    // Corrupt first 4 bytes (CRC)
    try (var raf = new RandomAccessFile(logFile.toFile(), "rw")) {
        raf.writeInt(0xDEADBEEF);
    }
    
    // Recover - should truncate entire file
    try (TopicLog log = new TopicLog(dir)) {
        assertEquals(0, log.size());
    }
}
```

---

## 8. Metrics & Observability

Add simple counters for debugging:

```java
public class Topic {
    
    private final AtomicLong publishCount = new AtomicLong();
    private final AtomicLong fetchCount = new AtomicLong();
    private final AtomicLong subscriberCount = new AtomicLong();
    
    public long publish(byte[] data) throws IOException {
        long offset = log.append(data);
        publishCount.incrementAndGet();
        broadcast(log.read(offset));
        return offset;
    }
    
    public void addSubscriber(ProtocolWriter writer, String clientId) {
        subscribers.add(new Subscriber(clientId, writer));
        subscriberCount.incrementAndGet();
    }
    
    public String getStats() {
        return String.format("Topic[%s]: published=%d, subscribers=%d, size=%d",
            name, publishCount.get(), subscriberCount.get(), log.size());
    }
}
```

---

## 9. Final Checklist

Before considering the broker "complete":

- [ ] All unit tests pass
- [ ] Recovery after kill -9 works
- [ ] Push subscribers receive messages
- [ ] Pull consumers can fetch batches
- [ ] Graceful shutdown flushes data
- [ ] No resource leaks (check with profiler)
- [ ] README with usage instructions

---

## 10. What You've Learned

| Java Concept | Where Used |
| :--- | :--- |
| Virtual Threads | `BrokerServer` - one per client |
| FileChannel | `LogSegment` - efficient file I/O |
| ByteBuffer | Protocol parsing |
| ConcurrentHashMap | `TopicRegistry` |
| ReentrantLock | Write serialization |
| CopyOnWriteArrayList | Subscriber management |
| CRC32 | Data integrity |
| try-with-resources | Everywhere |

**Congratulations!** 🎉 You've built a working message broker from scratch.
