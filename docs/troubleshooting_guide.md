# MiniBroker: Troubleshooting Guide

Common issues and how to debug them.

---

## Table of Contents

1. [Debugging Checklist](#1-debugging-checklist)
2. [Build & Compilation Issues](#2-build--compilation-issues)
3. [Runtime Errors](#3-runtime-errors)
4. [Network Issues](#4-network-issues)
5. [Data Corruption & Recovery](#5-data-corruption--recovery)
6. [Performance Issues](#6-performance-issues)
7. [Debugging Tools](#7-debugging-tools)

---

## 1. Debugging Checklist

Before diving into specific issues, check these common causes:

- [ ] Is Java 21+ installed? (`java --version`)
- [ ] Is the broker running? (`ps aux | grep minibroker`)
- [ ] Is the port available? (`lsof -i :9092`)
- [ ] Are there any firewall rules blocking connections?
- [ ] Is there enough disk space? (`df -h`)
- [ ] Check broker logs for errors

---

## 2. Build & Compilation Issues

### Issue: `Class file has wrong version`

**Error**:
```
java.lang.UnsupportedClassVersionError: minibroker/Main 
has been compiled by a more recent version of the Java Runtime (class file version 65.0)
```

**Cause**: Compiled with Java 21, running with older Java.

**Fix**:
```bash
java --version  # Check version
# If wrong, set JAVA_HOME
export JAVA_HOME=/path/to/java21
export PATH=$JAVA_HOME/bin:$PATH
```

### Issue: `package does not exist`

**Error**:
```
error: package org.junit.jupiter.api does not exist
```

**Cause**: Dependencies not downloaded.

**Fix**:
```bash
mvn clean install -DskipTests
```

### Issue: Maven not finding JDK

**Error**:
```
No compiler is provided in this environment
```

**Fix**: Ensure `JAVA_HOME` points to JDK (not JRE):
```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home
```

---

## 3. Runtime Errors

### Issue: `Address already in use`

**Error**:
```
java.net.BindException: Address already in use
```

**Cause**: Another process is using port 9092.

**Fix**:
```bash
# Find what's using the port
lsof -i :9092
# Kill it or use a different port
kill -9 <PID>
# Or change port in BrokerConfig
```

### Issue: `Connection refused`

**Error** (client-side):
```
java.net.ConnectException: Connection refused
```

**Cause**: Broker not running or wrong host/port.

**Fix**:
```bash
# Check if broker is listening
nc -zv localhost 9092
# Start broker if not running
mvn exec:java -Dexec.mainClass="minibroker.Main"
```

### Issue: `NullPointerException` in protocol parsing

**Error**:
```
java.lang.NullPointerException: Cannot read field "topic" because "request" is null
```

**Cause**: Client sent malformed request or disconnected.

**Fix**: Check client code for proper request formatting. Add null checks:
```java
Request request = reader.readRequest();
if (request == null) {
    // Client disconnected
    return;
}
```

### Issue: `BufferUnderflowException`

**Error**:
```
java.nio.BufferUnderflowException
```

**Cause**: Reading more bytes than available in ByteBuffer.

**Fix**: Check buffer state before reading:
```java
if (buffer.remaining() < 8) {
    throw new IOException("Not enough bytes for long");
}
long value = buffer.getLong();
```

### Issue: `IllegalArgumentException: Offset out of range`

**Error**:
```
java.lang.IllegalArgumentException: Offset 42 out of range [0, 10)
```

**Cause**: Consumer asking for offset that doesn't exist.

**Fix**: Handle gracefully:
```java
public List<Message> fetch(String topic, long offset, int count) {
    if (offset >= log.size()) {
        return Collections.emptyList(); // No messages available
    }
    // ...
}
```

---

## 4. Network Issues

### Issue: Client hangs on read

**Symptom**: Client blocks forever on `in.readInt()`.

**Cause**: Broker didn't send response or didn't flush.

**Fix** (broker-side):
```java
// Always flush after writing response
writer.writeAck(offset);
out.flush();  // ← Critical!
```

### Issue: Messages arrive partially

**Symptom**: Client receives incomplete data.

**Cause**: TCP stream fragmentation (normal) but client not handling it.

**Fix**: Always use `readFully()`:
```java
// BAD
byte[] data = new byte[size];
in.read(data);  // May read less than size!

// GOOD
byte[] data = new byte[size];
in.readFully(data);  // Blocks until all bytes received
```

### Issue: Subscriber stops receiving

**Symptom**: Push subscriber works initially, then stops.

**Cause**: Exception not logged, subscriber thread died.

**Fix**: Add error handling:
```java
Thread subThread = new Thread(() -> {
    try {
        subscriber.subscribe(topic, handler);
    } catch (Exception e) {
        System.err.println("Subscriber error: " + e);
        e.printStackTrace();
    }
});
```

---

## 5. Data Corruption & Recovery

### Issue: `CRC mismatch`

**Log output**:
```
CRC mismatch at position 12345
  Expected: 12345678, Got: 87654321
```

**Cause**: Data corruption (crash, disk error, bug).

**What happens**: Recovery truncates the file at corrupted position.

**Debug**: Inspect the log file:
```bash
# Hex dump around position 12345
xxd -s 12300 -l 100 data/my-topic/00000000000000000000.log
```

### Issue: `Negative size` during recovery

**Log output**:
```
Negative size at position 5678
```

**Cause**: Corrupted header or garbage data.

**What happens**: Recovery truncates the file.

**Prevention**: Ensure proper flush before ACK:
```java
public long append(byte[] data) {
    // ...write data...
    channel.force(true);  // Sync before returning
    return offset;
}
```

### Issue: All data lost after restart

**Symptom**: Topic exists but has 0 messages.

**Cause**: Complete file corruption (e.g., all zeros or garbage).

**Debug**:
```bash
# Check file size
ls -la data/my-topic/

# Check if file has content
head -c 100 data/my-topic/00000000000000000000.log | xxd
```

**Recovery**: If backup exists, restore from backup.

---

## 6. Performance Issues

### Issue: Slow writes

**Symptom**: Publishing takes >10ms per message.

**Cause**: `force(true)` on every write.

**Fix**: Use background flushing:
```java
// Periodic flush instead of per-write
ScheduledExecutorService flusher = Executors.newSingleThreadScheduledExecutor();
flusher.scheduleAtFixedRate(() -> {
    channel.force(true);
}, 500, 500, TimeUnit.MILLISECONDS);
```

### Issue: High CPU usage

**Symptom**: Broker uses 100% CPU.

**Cause**: Busy-waiting loop somewhere.

**Debug**: Thread dump:
```bash
jstack <PID> > thread_dump.txt
grep -A 20 "RUNNABLE" thread_dump.txt
```

**Common culprit**: Polling loop without sleep:
```java
// BAD
while (true) {
    if (queue.isEmpty()) {
        continue;  // Burns CPU
    }
}

// GOOD
while (true) {
    Message msg = queue.poll(100, TimeUnit.MILLISECONDS);
    if (msg != null) {
        process(msg);
    }
}
```

### Issue: Memory keeps growing

**Symptom**: Broker OOMs after running for a while.

**Cause**: Leaking references (e.g., not removing closed subscribers).

**Debug**: Heap dump:
```bash
jmap -dump:format=b,file=heap.hprof <PID>
# Analyze with Eclipse MAT or jhat
```

**Common culprit**: Not removing from subscriber list:
```java
// Must remove on disconnect
try {
    // ... handle messages
} finally {
    topic.removeSubscriber(writer);  // ← Critical!
}
```

### Issue: Slow reads

**Symptom**: Fetching messages takes seconds.

**Cause**: Large file, linear scan for each read.

**Fix**: Use in-memory offset index:
```java
// Instead of scanning file each time
List<Long> offsetIndex = new ArrayList<>();  // offset -> file position
// Populate on startup, update on append
```

---

## 7. Debugging Tools

### Log Inspection Tool

Create a utility to inspect log files:

```java
package minibroker.tools;

import java.nio.*;
import java.nio.channels.*;
import java.nio.file.*;

public class LogInspector {
    
    private static final int HEADER_SIZE = 24;
    
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Usage: LogInspector <log-file>");
            return;
        }
        
        Path logFile = Path.of(args[0]);
        try (FileChannel channel = FileChannel.open(logFile, StandardOpenOption.READ)) {
            
            long position = 0;
            long fileSize = channel.size();
            int entryCount = 0;
            
            System.out.println("File: " + logFile);
            System.out.println("Size: " + fileSize + " bytes");
            System.out.println("─".repeat(60));
            
            while (position + HEADER_SIZE <= fileSize) {
                ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
                channel.read(header, position);
                header.flip();
                
                int crc = header.getInt();
                long offset = header.getLong();
                long timestamp = header.getLong();
                int size = header.getInt();
                
                System.out.printf("Entry %d at position %d:%n", entryCount, position);
                System.out.printf("  Offset:    %d%n", offset);
                System.out.printf("  Timestamp: %d (%s)%n", timestamp, 
                    java.time.Instant.ofEpochMilli(timestamp));
                System.out.printf("  Size:      %d bytes%n", size);
                System.out.printf("  CRC:       0x%08X%n", crc);
                
                if (size > 0 && position + HEADER_SIZE + size <= fileSize) {
                    ByteBuffer payload = ByteBuffer.allocate(Math.min(size, 100));
                    channel.read(payload, position + HEADER_SIZE);
                    payload.flip();
                    byte[] preview = new byte[payload.remaining()];
                    payload.get(preview);
                    
                    String text = new String(preview);
                    if (size > 100) {
                        text += "... (truncated)";
                    }
                    System.out.printf("  Payload:   %s%n", text.replaceAll("[\\r\\n]", " "));
                }
                
                System.out.println();
                position += HEADER_SIZE + size;
                entryCount++;
            }
            
            System.out.println("─".repeat(60));
            System.out.println("Total entries: " + entryCount);
            
            if (position < fileSize) {
                System.out.println("WARNING: " + (fileSize - position) + " bytes of garbage at end");
            }
        }
    }
}
```

Run it:
```bash
mvn compile exec:java -Dexec.mainClass="minibroker.tools.LogInspector" \
  -Dexec.args="data/my-topic/00000000000000000000.log"
```

### Protocol Trace Tool

Add protocol debugging:

```java
// In ProtocolReader
public Request readRequest() throws IOException {
    int length = in.readInt();
    byte cmdByte = in.readByte();
    
    System.out.printf("[TRACE] Received: len=%d, cmd=0x%02X%n", length, cmdByte);
    
    // ... rest of parsing
}

// In ProtocolWriter
public void writeAck(long offset) throws IOException {
    System.out.printf("[TRACE] Sending ACK: offset=%d%n", offset);
    
    // ... rest of writing
}
```

### Common Debug Outputs

```java
// Add to BrokerServer
public void start() {
    System.out.println("[DEBUG] Starting broker on port " + port);
    System.out.println("[DEBUG] Data directory: " + dataDir);
    System.out.println("[DEBUG] Java version: " + System.getProperty("java.version"));
    // ...
}

// Add to ClientHandler
public void run() {
    String clientAddr = socket.getRemoteSocketAddress().toString();
    System.out.println("[DEBUG] Client connected: " + clientAddr);
    
    try {
        // ...
    } finally {
        System.out.println("[DEBUG] Client disconnected: " + clientAddr);
    }
}

// Add to TopicLog
public long append(byte[] data) {
    long offset = segment.append(data);
    System.out.printf("[DEBUG] Appended %d bytes at offset %d%n", data.length, offset);
    return offset;
}
```

---

## Quick Fixes Reference

| Symptom | Likely Cause | Quick Fix |
| :--- | :--- | :--- |
| Address already in use | Port busy | Kill other process or change port |
| Connection refused | Broker not running | Start broker |
| Client hangs | Missing flush | Add `out.flush()` |
| Partial reads | Not using readFully | Use `in.readFully()` |
| CRC mismatch | Corruption | Let recovery handle it |
| Slow writes | Force every write | Use periodic flush |
| Memory leak | Not removing subscribers | Clean up in finally block |
| CPU 100% | Busy loop | Add sleep in polling |
