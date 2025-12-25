# MiniBroker: Testing Guide

Comprehensive testing strategies for the MiniBroker project.

---

## Table of Contents

1. [Testing Philosophy](#1-testing-philosophy)
2. [Project Test Setup](#2-project-test-setup)
3. [Unit Testing the Log Engine](#3-unit-testing-the-log-engine)
4. [Unit Testing the Protocol Layer](#4-unit-testing-the-protocol-layer)
5. [Integration Testing](#5-integration-testing)
6. [Chaos Testing](#6-chaos-testing)
7. [Performance Testing](#7-performance-testing)

---

## 1. Testing Philosophy

### What to Test

| Component | Test Type | Focus |
| :--- | :--- | :--- |
| LogEntry | Unit | Immutability, equals/hashCode |
| LogSegment | Unit | Append, read, recovery, CRC |
| Protocol | Unit | Serialization round-trips |
| Server | Integration | End-to-end flows |
| Recovery | Chaos | Crash scenarios |

### Testing Pyramid

```
          ╱ ╲
         ╱   ╲        ← Integration/E2E (few)
        ╱─────╲
       ╱       ╲
      ╱─────────╲     ← Integration (some)
     ╱           ╲
    ╱─────────────╲   ← Unit tests (many)
   ╱               ╲
  ╱─────────────────╲
```

---

## 2. Project Test Setup

### Add JUnit 5 to pom.xml

```xml
<dependencies>
    <dependency>
        <groupId>org.junit.jupiter</groupId>
        <artifactId>junit-jupiter</artifactId>
        <version>5.10.1</version>
        <scope>test</scope>
    </dependency>
    
    <!-- For assertions -->
    <dependency>
        <groupId>org.assertj</groupId>
        <artifactId>assertj-core</artifactId>
        <version>3.24.2</version>
        <scope>test</scope>
    </dependency>
</dependencies>

<build>
    <plugins>
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-surefire-plugin</artifactId>
            <version>3.2.2</version>
        </plugin>
    </plugins>
</build>
```

### Test Directory Structure

```
src/test/java/minibroker/
├── log/
│   ├── LogEntryTest.java
│   ├── LogSegmentTest.java
│   └── TopicLogTest.java
├── protocol/
│   ├── ProtocolReaderTest.java
│   ├── ProtocolWriterTest.java
│   └── ProtocolRoundTripTest.java
├── server/
│   └── BrokerIntegrationTest.java
├── topic/
│   └── TopicTest.java
└── chaos/
    └── RecoveryTest.java
```

### Running Tests

```bash
# Run all tests
mvn test

# Run specific test class
mvn test -Dtest=LogSegmentTest

# Run specific test method
mvn test -Dtest=LogSegmentTest#testAppendAndRead

# Run with verbose output
mvn test -X
```

---

## 3. Unit Testing the Log Engine

### LogEntryTest.java

```java
package minibroker.log;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class LogEntryTest {
    
    @Test
    void constructorValidatesOffset() {
        assertThrows(IllegalArgumentException.class, () -> 
            new LogEntry(-1, 0, new byte[0])
        );
    }
    
    @Test
    void constructorValidatesPayload() {
        assertThrows(NullPointerException.class, () -> 
            new LogEntry(0, 0, null)
        );
    }
    
    @Test
    void payloadIsDefensivelyCopied() {
        byte[] original = {1, 2, 3};
        LogEntry entry = new LogEntry(0, 0, original);
        
        // Modify original
        original[0] = 99;
        
        // Entry should be unchanged
        assertEquals(1, entry.payload()[0]);
    }
    
    @Test
    void payloadReturnIsDefensivelyCopied() {
        LogEntry entry = new LogEntry(0, 0, new byte[]{1, 2, 3});
        
        // Modify returned array
        entry.payload()[0] = 99;
        
        // Next call should still return original
        assertEquals(1, entry.payload()[0]);
    }
    
    @Test
    void equalsAndHashCode() {
        LogEntry e1 = new LogEntry(0, 100, new byte[]{1, 2, 3});
        LogEntry e2 = new LogEntry(0, 100, new byte[]{1, 2, 3});
        LogEntry e3 = new LogEntry(1, 100, new byte[]{1, 2, 3});
        
        assertEquals(e1, e2);
        assertEquals(e1.hashCode(), e2.hashCode());
        assertNotEquals(e1, e3);
    }
}
```

### LogSegmentTest.java

```java
package minibroker.log;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.io.RandomAccessFile;

import static org.junit.jupiter.api.Assertions.*;

class LogSegmentTest {
    
    @TempDir
    Path tempDir;
    
    private Path logFile;
    
    @BeforeEach
    void setUp() {
        logFile = tempDir.resolve("test.log");
    }
    
    @Test
    @DisplayName("Append single message and read it back")
    void appendAndReadSingle() throws Exception {
        try (LogSegment segment = new LogSegment(logFile)) {
            byte[] payload = "Hello, World!".getBytes();
            
            long offset = segment.append(payload);
            
            assertEquals(0, offset);
            
            LogEntry entry = segment.read(0);
            assertArrayEquals(payload, entry.payload());
            assertEquals(0, entry.offset());
            assertTrue(entry.timestamp() > 0);
        }
    }
    
    @Test
    @DisplayName("Append multiple messages sequentially")
    void appendMultiple() throws Exception {
        try (LogSegment segment = new LogSegment(logFile)) {
            for (int i = 0; i < 100; i++) {
                long offset = segment.append(("Message " + i).getBytes());
                assertEquals(i, offset);
            }
            
            assertEquals(100, segment.size());
            
            // Read all back
            for (int i = 0; i < 100; i++) {
                LogEntry entry = segment.read(i);
                assertEquals("Message " + i, new String(entry.payload()));
            }
        }
    }
    
    @Test
    @DisplayName("Reading invalid offset throws exception")
    void readInvalidOffset() throws Exception {
        try (LogSegment segment = new LogSegment(logFile)) {
            segment.append("test".getBytes());
            
            assertThrows(IllegalArgumentException.class, () -> segment.read(-1));
            assertThrows(IllegalArgumentException.class, () -> segment.read(1));
            assertThrows(IllegalArgumentException.class, () -> segment.read(100));
        }
    }
    
    @Test
    @DisplayName("Data persists across close and reopen")
    void persistence() throws Exception {
        // Write
        try (LogSegment segment = new LogSegment(logFile)) {
            segment.append("First".getBytes());
            segment.append("Second".getBytes());
            segment.append("Third".getBytes());
            segment.flush();
        }
        
        // Reopen and read
        try (LogSegment segment = new LogSegment(logFile)) {
            assertEquals(3, segment.size());
            assertEquals("First", new String(segment.read(0).payload()));
            assertEquals("Second", new String(segment.read(1).payload()));
            assertEquals("Third", new String(segment.read(2).payload()));
        }
    }
    
    @Test
    @DisplayName("Large payloads are handled correctly")
    void largePayload() throws Exception {
        try (LogSegment segment = new LogSegment(logFile)) {
            // 1MB payload
            byte[] largePayload = new byte[1024 * 1024];
            for (int i = 0; i < largePayload.length; i++) {
                largePayload[i] = (byte) (i % 256);
            }
            
            long offset = segment.append(largePayload);
            assertEquals(0, offset);
            
            LogEntry entry = segment.read(0);
            assertArrayEquals(largePayload, entry.payload());
        }
    }
    
    @Test
    @DisplayName("Empty payload is allowed")
    void emptyPayload() throws Exception {
        try (LogSegment segment = new LogSegment(logFile)) {
            long offset = segment.append(new byte[0]);
            assertEquals(0, offset);
            
            LogEntry entry = segment.read(0);
            assertEquals(0, entry.payload().length);
        }
    }
    
    @Test
    @DisplayName("Recovery after incomplete header write")
    void recoveryIncompleteHeader() throws Exception {
        // Write valid entries
        try (LogSegment segment = new LogSegment(logFile)) {
            segment.append("Valid1".getBytes());
            segment.append("Valid2".getBytes());
            segment.flush();
        }
        
        // Append incomplete header (10 bytes, header needs 24)
        try (RandomAccessFile raf = new RandomAccessFile(logFile.toFile(), "rw")) {
            raf.seek(raf.length());
            raf.write(new byte[10]); // Incomplete header
        }
        
        // Recovery should truncate and restore valid entries
        try (LogSegment segment = new LogSegment(logFile)) {
            assertEquals(2, segment.size());
            assertEquals("Valid1", new String(segment.read(0).payload()));
            assertEquals("Valid2", new String(segment.read(1).payload()));
        }
    }
    
    @Test
    @DisplayName("Recovery after CRC corruption")
    void recoveryCRCCorruption() throws Exception {
        // Write valid entry
        try (LogSegment segment = new LogSegment(logFile)) {
            segment.append("ValidMessage".getBytes());
            segment.flush();
        }
        
        // Corrupt the CRC (first 4 bytes)
        try (RandomAccessFile raf = new RandomAccessFile(logFile.toFile(), "rw")) {
            raf.writeInt(0xDEADBEEF);
        }
        
        // Recovery should truncate
        try (LogSegment segment = new LogSegment(logFile)) {
            assertEquals(0, segment.size());
        }
    }
    
    @Test
    @DisplayName("Recovery after partial payload write")
    void recoveryPartialPayload() throws Exception {
        // Write valid entries
        try (LogSegment segment = new LogSegment(logFile)) {
            segment.append("Valid".getBytes());
            segment.flush();
        }
        
        long fileSize;
        try (RandomAccessFile raf = new RandomAccessFile(logFile.toFile(), "r")) {
            fileSize = raf.length();
        }
        
        // Append header for 100-byte payload, but only write 50 bytes
        try (RandomAccessFile raf = new RandomAccessFile(logFile.toFile(), "rw")) {
            raf.seek(fileSize);
            raf.writeInt(0);    // CRC (fake)
            raf.writeLong(1);   // Offset
            raf.writeLong(System.currentTimeMillis()); // Timestamp
            raf.writeInt(100);  // Size claims 100 bytes
            raf.write(new byte[50]); // But only 50 bytes written
        }
        
        // Recovery should truncate incomplete entry
        try (LogSegment segment = new LogSegment(logFile)) {
            assertEquals(1, segment.size());
            assertEquals("Valid", new String(segment.read(0).payload()));
        }
    }
}
```

### TopicLogTest.java

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
    @DisplayName("Topic directory is created automatically")
    void createsDirectory() throws Exception {
        Path topicDir = tempDir.resolve("new-topic");
        
        try (TopicLog log = new TopicLog(topicDir)) {
            assertTrue(topicDir.toFile().exists());
            assertTrue(topicDir.toFile().isDirectory());
        }
    }
    
    @Test
    @DisplayName("Concurrent appends are thread-safe")
    void concurrentAppends() throws Exception {
        try (TopicLog log = new TopicLog(tempDir.resolve("concurrent"))) {
            int threadCount = 10;
            int messagesPerThread = 100;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);
            ConcurrentHashMap<Long, String> offsets = new ConcurrentHashMap<>();
            
            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    try {
                        startLatch.await(); // Wait for signal
                        for (int m = 0; m < messagesPerThread; m++) {
                            String msg = "T" + threadId + "-M" + m;
                            long offset = log.append(msg.getBytes());
                            offsets.put(offset, msg);
                        }
                    } catch (Exception e) {
                        fail(e);
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }
            
            startLatch.countDown(); // Start all threads
            doneLatch.await(30, TimeUnit.SECONDS);
            executor.shutdown();
            
            // Verify
            assertEquals(threadCount * messagesPerThread, log.size());
            assertEquals(threadCount * messagesPerThread, offsets.size());
            
            // Every offset should be unique and valid
            for (var entry : offsets.entrySet()) {
                LogEntry logEntry = log.read(entry.getKey());
                assertEquals(entry.getValue(), new String(logEntry.payload()));
            }
        }
    }
}
```

---

## 4. Unit Testing the Protocol Layer

### ProtocolRoundTripTest.java

```java
package minibroker.protocol;

import org.junit.jupiter.api.Test;
import java.io.*;

import static org.junit.jupiter.api.Assertions.*;

class ProtocolRoundTripTest {
    
    @Test
    void publishRequestRoundTrip() throws Exception {
        // Create request
        PublishRequest original = new PublishRequest(
            "orders", 
            "Hello, Broker!".getBytes()
        );
        
        // Serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        writePublishRequest(baos, original);
        
        // Deserialize
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ProtocolReader reader = new ProtocolReader(bais);
        Request parsed = reader.readRequest();
        
        // Verify
        assertInstanceOf(PublishRequest.class, parsed);
        PublishRequest pub = (PublishRequest) parsed;
        assertEquals("orders", pub.topic());
        assertArrayEquals(original.data(), pub.data());
    }
    
    @Test
    void fetchRequestRoundTrip() throws Exception {
        FetchRequest original = new FetchRequest("events", 42, 100);
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        writeFetchRequest(baos, original);
        
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ProtocolReader reader = new ProtocolReader(bais);
        Request parsed = reader.readRequest();
        
        assertInstanceOf(FetchRequest.class, parsed);
        FetchRequest fetch = (FetchRequest) parsed;
        assertEquals("events", fetch.topic());
        assertEquals(42, fetch.startOffset());
        assertEquals(100, fetch.maxCount());
    }
    
    @Test
    void subscribeRequestRoundTrip() throws Exception {
        SubscribeRequest original = new SubscribeRequest("notifications");
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        writeSubscribeRequest(baos, original);
        
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ProtocolReader reader = new ProtocolReader(bais);
        Request parsed = reader.readRequest();
        
        assertInstanceOf(SubscribeRequest.class, parsed);
        assertEquals("notifications", ((SubscribeRequest) parsed).topic());
    }
    
    // Helper methods to write requests (simulate client)
    private void writePublishRequest(OutputStream os, PublishRequest req) throws IOException {
        DataOutputStream out = new DataOutputStream(os);
        byte[] topic = req.topic().getBytes();
        byte[] data = req.data();
        
        int payloadLen = 2 + topic.length + 4 + data.length;
        out.writeInt(1 + payloadLen);
        out.writeByte(Command.PUBLISH.code());
        out.writeShort(topic.length);
        out.write(topic);
        out.writeInt(data.length);
        out.write(data);
        out.flush();
    }
    
    private void writeFetchRequest(OutputStream os, FetchRequest req) throws IOException {
        DataOutputStream out = new DataOutputStream(os);
        byte[] topic = req.topic().getBytes();
        
        int payloadLen = 2 + topic.length + 8 + 4;
        out.writeInt(1 + payloadLen);
        out.writeByte(Command.FETCH.code());
        out.writeShort(topic.length);
        out.write(topic);
        out.writeLong(req.startOffset());
        out.writeInt(req.maxCount());
        out.flush();
    }
    
    private void writeSubscribeRequest(OutputStream os, SubscribeRequest req) throws IOException {
        DataOutputStream out = new DataOutputStream(os);
        byte[] topic = req.topic().getBytes();
        
        int payloadLen = 2 + topic.length;
        out.writeInt(1 + payloadLen);
        out.writeByte(Command.SUBSCRIBE.code());
        out.writeShort(topic.length);
        out.write(topic);
        out.flush();
    }
}
```

---

## 5. Integration Testing

### BrokerIntegrationTest.java

```java
package minibroker.server;

import minibroker.topic.TopicRegistry;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.io.*;
import java.net.Socket;
import java.nio.file.Path;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class BrokerIntegrationTest {
    
    @TempDir
    Path tempDir;
    
    private BrokerServer server;
    private Thread serverThread;
    private int port = 19092; // Use non-standard port for tests
    
    @BeforeEach
    void startServer() throws Exception {
        TopicRegistry registry = new TopicRegistry(tempDir);
        server = new BrokerServer(port, registry);
        
        serverThread = new Thread(() -> {
            try {
                server.start();
            } catch (IOException e) {
                // Server stopped
            }
        });
        serverThread.start();
        
        // Wait for server to be ready
        Thread.sleep(500);
    }
    
    @AfterEach
    void stopServer() {
        server.stop();
        serverThread.interrupt();
    }
    
    @Test
    @DisplayName("Publish and fetch round-trip")
    void publishAndFetch() throws Exception {
        String topic = "test-topic";
        String message = "Hello, MiniBroker!";
        
        // Publish
        try (Socket socket = new Socket("localhost", port);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {
            
            writePublish(out, topic, message.getBytes());
            
            // Read ACK
            int len = in.readInt();
            byte cmd = in.readByte();
            assertEquals((byte) 0x81, cmd); // ACK
            long offset = in.readLong();
            assertEquals(0, offset);
        }
        
        // Fetch
        try (Socket socket = new Socket("localhost", port);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {
            
            writeFetch(out, topic, 0, 10);
            
            // Read BATCH
            int len = in.readInt();
            byte cmd = in.readByte();
            assertEquals((byte) 0x83, cmd); // BATCH
            
            int count = in.readInt();
            assertEquals(1, count);
            
            long offset = in.readLong();
            long timestamp = in.readLong();
            int size = in.readInt();
            byte[] payload = new byte[size];
            in.readFully(payload);
            
            assertEquals(0, offset);
            assertEquals(message, new String(payload));
        }
    }
    
    @Test
    @DisplayName("Multiple clients can publish concurrently")
    void concurrentPublish() throws Exception {
        int clientCount = 5;
        int messagesPerClient = 20;
        String topic = "concurrent-topic";
        
        ExecutorService executor = Executors.newFixedThreadPool(clientCount);
        CountDownLatch latch = new CountDownLatch(clientCount);
        
        for (int c = 0; c < clientCount; c++) {
            final int clientId = c;
            executor.submit(() -> {
                try (Socket socket = new Socket("localhost", port);
                     DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                     DataInputStream in = new DataInputStream(socket.getInputStream())) {
                    
                    for (int m = 0; m < messagesPerClient; m++) {
                        String msg = "Client" + clientId + "-Msg" + m;
                        writePublish(out, topic, msg.getBytes());
                        
                        // Read ACK
                        in.readInt();
                        in.readByte();
                        in.readLong();
                    }
                } catch (Exception e) {
                    fail(e);
                } finally {
                    latch.countDown();
                }
            });
        }
        
        latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();
        
        // Verify all messages
        try (Socket socket = new Socket("localhost", port);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {
            
            writeFetch(out, topic, 0, 1000);
            
            in.readInt();
            in.readByte();
            int count = in.readInt();
            
            assertEquals(clientCount * messagesPerClient, count);
        }
    }
    
    // Helper methods
    private void writePublish(DataOutputStream out, String topic, byte[] data) throws IOException {
        byte[] topicBytes = topic.getBytes();
        int payloadLen = 2 + topicBytes.length + 4 + data.length;
        out.writeInt(1 + payloadLen);
        out.writeByte(0x01);
        out.writeShort(topicBytes.length);
        out.write(topicBytes);
        out.writeInt(data.length);
        out.write(data);
        out.flush();
    }
    
    private void writeFetch(DataOutputStream out, String topic, long offset, int count) throws IOException {
        byte[] topicBytes = topic.getBytes();
        int payloadLen = 2 + topicBytes.length + 8 + 4;
        out.writeInt(1 + payloadLen);
        out.writeByte(0x03);
        out.writeShort(topicBytes.length);
        out.write(topicBytes);
        out.writeLong(offset);
        out.writeInt(count);
        out.flush();
    }
}
```

---

## 6. Chaos Testing

### RecoveryTest.java

Tests that simulate crashes and verify recovery.

```java
package minibroker.chaos;

import minibroker.log.TopicLog;
import minibroker.log.LogEntry;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.io.RandomAccessFile;
import java.nio.file.*;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class RecoveryTest {
    
    @TempDir
    Path tempDir;
    
    @Test
    @DisplayName("Crash after random number of bytes written")
    @RepeatedTest(10) // Run multiple times for randomness
    void crashAtRandomPoint() throws Exception {
        Path topicDir = tempDir.resolve("crash-" + System.nanoTime());
        Path logFile = topicDir.resolve("00000000000000000000.log");
        Random random = new Random();
        
        // Write some valid messages
        int messageCount = 10 + random.nextInt(90);
        try (TopicLog log = new TopicLog(topicDir)) {
            for (int i = 0; i < messageCount; i++) {
                log.append(("Message-" + i).getBytes());
            }
            log.flush();
        }
        
        long originalSize;
        try (RandomAccessFile raf = new RandomAccessFile(logFile.toFile(), "r")) {
            originalSize = raf.length();
        }
        
        // Append random garbage to simulate crash
        int garbageSize = random.nextInt(1000);
        try (RandomAccessFile raf = new RandomAccessFile(logFile.toFile(), "rw")) {
            raf.seek(raf.length());
            byte[] garbage = new byte[garbageSize];
            random.nextBytes(garbage);
            raf.write(garbage);
        }
        
        // Recovery should restore all valid messages
        try (TopicLog log = new TopicLog(topicDir)) {
            assertEquals(messageCount, log.size(), 
                "Should recover all " + messageCount + " messages");
            
            // Verify each message
            for (int i = 0; i < messageCount; i++) {
                LogEntry entry = log.read(i);
                assertEquals("Message-" + i, new String(entry.payload()));
            }
        }
    }
    
    @Test
    @DisplayName("File truncated mid-message")
    void fileTruncated() throws Exception {
        Path topicDir = tempDir.resolve("truncate-test");
        Path logFile = topicDir.resolve("00000000000000000000.log");
        
        // Write messages
        try (TopicLog log = new TopicLog(topicDir)) {
            log.append("First".getBytes());
            log.append("Second".getBytes());
            log.append("Third".getBytes());
            log.flush();
        }
        
        // Truncate file mid-way through last message
        try (RandomAccessFile raf = new RandomAccessFile(logFile.toFile(), "rw")) {
            raf.setLength(raf.length() - 3); // Remove last 3 bytes
        }
        
        // Should recover first 2 messages
        try (TopicLog log = new TopicLog(topicDir)) {
            assertEquals(2, log.size());
            assertEquals("First", new String(log.read(0).payload()));
            assertEquals("Second", new String(log.read(1).payload()));
        }
    }
    
    @Test
    @DisplayName("Complete file corruption recovers to empty")
    void completeCorruption() throws Exception {
        Path topicDir = tempDir.resolve("corrupt-test");
        Path logFile = topicDir.resolve("00000000000000000000.log");
        
        // Write messages
        try (TopicLog log = new TopicLog(topicDir)) {
            log.append("Message".getBytes());
            log.flush();
        }
        
        // Overwrite with garbage
        try (RandomAccessFile raf = new RandomAccessFile(logFile.toFile(), "rw")) {
            byte[] garbage = new byte[(int) raf.length()];
            new Random().nextBytes(garbage);
            raf.seek(0);
            raf.write(garbage);
        }
        
        // Should recover to empty
        try (TopicLog log = new TopicLog(topicDir)) {
            assertEquals(0, log.size());
        }
    }
}
```

---

## 7. Performance Testing

### BenchmarkTest.java

```java
package minibroker.benchmark;

import minibroker.log.TopicLog;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.concurrent.*;

class BenchmarkTest {
    
    @TempDir
    Path tempDir;
    
    @Test
    @DisplayName("Throughput: Sequential writes")
    void sequentialWriteThroughput() throws Exception {
        int messageCount = 100_000;
        byte[] payload = new byte[100]; // 100 byte messages
        
        try (TopicLog log = new TopicLog(tempDir.resolve("bench"))) {
            long start = System.nanoTime();
            
            for (int i = 0; i < messageCount; i++) {
                log.append(payload);
            }
            log.flush();
            
            long end = System.nanoTime();
            double seconds = (end - start) / 1_000_000_000.0;
            double throughput = messageCount / seconds;
            double mbps = (messageCount * payload.length) / seconds / 1_000_000.0;
            
            System.out.printf("Sequential writes: %.2f msgs/sec, %.2f MB/sec%n", 
                throughput, mbps);
        }
    }
    
    @Test
    @DisplayName("Throughput: Concurrent writes")
    void concurrentWriteThroughput() throws Exception {
        int threadCount = 4;
        int messagesPerThread = 25_000;
        byte[] payload = new byte[100];
        
        try (TopicLog log = new TopicLog(tempDir.resolve("bench-concurrent"))) {
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch latch = new CountDownLatch(threadCount);
            
            long start = System.nanoTime();
            
            for (int t = 0; t < threadCount; t++) {
                executor.submit(() -> {
                    try {
                        for (int i = 0; i < messagesPerThread; i++) {
                            log.append(payload);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                });
            }
            
            latch.await();
            log.flush();
            
            long end = System.nanoTime();
            double seconds = (end - start) / 1_000_000_000.0;
            int totalMessages = threadCount * messagesPerThread;
            double throughput = totalMessages / seconds;
            
            System.out.printf("Concurrent writes (%d threads): %.2f msgs/sec%n", 
                threadCount, throughput);
            
            executor.shutdown();
        }
    }
    
    @Test
    @DisplayName("Latency: Single write")
    void writeLatency() throws Exception {
        byte[] payload = new byte[100];
        int warmup = 1000;
        int measurements = 10_000;
        long[] latencies = new long[measurements];
        
        try (TopicLog log = new TopicLog(tempDir.resolve("bench-latency"))) {
            // Warmup
            for (int i = 0; i < warmup; i++) {
                log.append(payload);
            }
            
            // Measure
            for (int i = 0; i < measurements; i++) {
                long start = System.nanoTime();
                log.append(payload);
                latencies[i] = System.nanoTime() - start;
            }
            
            // Calculate percentiles
            java.util.Arrays.sort(latencies);
            long p50 = latencies[measurements / 2];
            long p99 = latencies[(int) (measurements * 0.99)];
            long p999 = latencies[(int) (measurements * 0.999)];
            
            System.out.printf("Write latency: p50=%.2fµs, p99=%.2fµs, p99.9=%.2fµs%n",
                p50 / 1000.0, p99 / 1000.0, p999 / 1000.0);
        }
    }
}
```

---

## Test Execution Summary

```bash
# Run all tests
mvn test

# Run only unit tests (fast)
mvn test -Dtest="*Test" -DexcludedGroups=integration

# Run integration tests
mvn test -Dtest="*IntegrationTest"

# Run benchmarks (takes longer)
mvn test -Dtest="BenchmarkTest"

# Generate test coverage report (add JaCoCo plugin)
mvn test jacoco:report
```
