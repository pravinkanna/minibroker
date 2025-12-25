# MiniBroker: Client SDK Guide

How to build a proper client library for MiniBroker.

---

## Table of Contents

1. [Client Architecture](#1-client-architecture)
2. [Producer Client](#2-producer-client)
3. [Pull Consumer Client](#3-pull-consumer-client)
4. [Push Consumer Client](#4-push-consumer-client)
5. [Connection Management](#5-connection-management)
6. [Error Handling](#6-error-handling)
7. [Best Practices](#7-best-practices)

---

## 1. Client Architecture

### Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                       MiniBrokerClient                          │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │  Producer   │  │PullConsumer │  │     PushConsumer        │  │
│  └──────┬──────┘  └──────┬──────┘  └───────────┬─────────────┘  │
│         │                │                     │                │
│         └────────────────┼─────────────────────┘                │
│                          │                                      │
│                   ┌──────▼──────┐                               │
│                   │ Connection  │                               │
│                   │   Manager   │                               │
│                   └──────┬──────┘                               │
│                          │                                      │
│                   ┌──────▼──────┐                               │
│                   │   Socket    │                               │
│                   └─────────────┘                               │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. Producer Client

### MiniBrokerProducer.java

```java
package minibroker.client;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Thread-safe producer client for MiniBroker.
 * 
 * Usage:
 * <pre>
 * try (var producer = new MiniBrokerProducer("localhost", 9092)) {
 *     long offset = producer.publish("orders", "order-123".getBytes());
 *     System.out.println("Published at offset: " + offset);
 * }
 * </pre>
 */
public class MiniBrokerProducer implements AutoCloseable {
    
    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;
    private final ReentrantLock lock = new ReentrantLock();
    
    public MiniBrokerProducer(String host, int port) throws IOException {
        this.socket = new Socket(host, port);
        this.socket.setTcpNoDelay(true); // Disable Nagle for low latency
        this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
    }
    
    /**
     * Publish a message to a topic.
     * 
     * @param topic The topic name
     * @param data The message payload
     * @return The offset assigned by the broker
     * @throws IOException if communication fails
     * @throws BrokerException if broker returns an error
     */
    public long publish(String topic, byte[] data) throws IOException, BrokerException {
        lock.lock();
        try {
            // Build and send request
            byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
            int payloadLen = 2 + topicBytes.length + 4 + data.length;
            
            out.writeInt(1 + payloadLen);
            out.writeByte(0x01); // PUBLISH
            out.writeShort(topicBytes.length);
            out.write(topicBytes);
            out.writeInt(data.length);
            out.write(data);
            out.flush();
            
            // Read response
            int respLen = in.readInt();
            byte cmd = in.readByte();
            
            if (cmd == (byte) 0x81) { // ACK
                return in.readLong();
            } else if (cmd == (byte) 0xFF) { // ERROR
                int code = in.readUnsignedShort();
                int msgLen = in.readUnsignedShort();
                byte[] msgBytes = new byte[msgLen];
                in.readFully(msgBytes);
                throw new BrokerException(code, new String(msgBytes, StandardCharsets.UTF_8));
            } else {
                throw new IOException("Unexpected response: 0x" + Integer.toHexString(cmd & 0xFF));
            }
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * Publish a string message (convenience method).
     */
    public long publish(String topic, String message) throws IOException, BrokerException {
        return publish(topic, message.getBytes(StandardCharsets.UTF_8));
    }
    
    @Override
    public void close() throws IOException {
        socket.close();
    }
}
```

### Producer with Batching

```java
package minibroker.client;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Producer that batches messages for higher throughput.
 */
public class BatchingProducer implements AutoCloseable {
    
    private final MiniBrokerProducer producer;
    private final Map<String, List<PendingMessage>> batches = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler;
    private final int batchSize;
    private final long lingerMs;
    
    public BatchingProducer(String host, int port, int batchSize, long lingerMs) throws IOException {
        this.producer = new MiniBrokerProducer(host, port);
        this.batchSize = batchSize;
        this.lingerMs = lingerMs;
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        
        // Schedule periodic flush
        scheduler.scheduleAtFixedRate(this::flushAll, lingerMs, lingerMs, TimeUnit.MILLISECONDS);
    }
    
    public CompletableFuture<Long> publishAsync(String topic, byte[] data) {
        PendingMessage pending = new PendingMessage(data, new CompletableFuture<>());
        
        batches.computeIfAbsent(topic, k -> Collections.synchronizedList(new ArrayList<>()))
               .add(pending);
        
        // Check if batch is full
        List<PendingMessage> batch = batches.get(topic);
        if (batch.size() >= batchSize) {
            flush(topic);
        }
        
        return pending.future;
    }
    
    private void flush(String topic) {
        List<PendingMessage> batch = batches.remove(topic);
        if (batch == null || batch.isEmpty()) return;
        
        // Send all messages in batch
        for (PendingMessage msg : batch) {
            try {
                long offset = producer.publish(topic, msg.data);
                msg.future.complete(offset);
            } catch (Exception e) {
                msg.future.completeExceptionally(e);
            }
        }
    }
    
    private void flushAll() {
        for (String topic : batches.keySet()) {
            flush(topic);
        }
    }
    
    @Override
    public void close() throws IOException {
        scheduler.shutdown();
        flushAll();
        producer.close();
    }
    
    private static class PendingMessage {
        final byte[] data;
        final CompletableFuture<Long> future;
        
        PendingMessage(byte[] data, CompletableFuture<Long> future) {
            this.data = data;
            this.future = future;
        }
    }
}
```

---

## 3. Pull Consumer Client

### MiniBrokerConsumer.java

```java
package minibroker.client;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Pull-based consumer for MiniBroker.
 * 
 * Usage:
 * <pre>
 * try (var consumer = new MiniBrokerConsumer("localhost", 9092)) {
 *     long offset = 0;
 *     while (true) {
 *         List<Message> messages = consumer.fetch("orders", offset, 100);
 *         for (Message msg : messages) {
 *             process(msg);
 *             offset = msg.offset() + 1;
 *         }
 *         saveOffset(offset); // Persist for recovery
 *     }
 * }
 * </pre>
 */
public class MiniBrokerConsumer implements AutoCloseable {
    
    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;
    
    public MiniBrokerConsumer(String host, int port) throws IOException {
        this.socket = new Socket(host, port);
        this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
    }
    
    /**
     * Fetch messages from a topic.
     * 
     * @param topic The topic to fetch from
     * @param startOffset Starting offset (inclusive)
     * @param maxCount Maximum number of messages to fetch
     * @return List of messages (may be empty if no messages available)
     */
    public List<Message> fetch(String topic, long startOffset, int maxCount) 
            throws IOException, BrokerException {
        
        // Send FETCH request
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        int payloadLen = 2 + topicBytes.length + 8 + 4;
        
        out.writeInt(1 + payloadLen);
        out.writeByte(0x03); // FETCH
        out.writeShort(topicBytes.length);
        out.write(topicBytes);
        out.writeLong(startOffset);
        out.writeInt(maxCount);
        out.flush();
        
        // Read response
        int respLen = in.readInt();
        byte cmd = in.readByte();
        
        if (cmd == (byte) 0x83) { // BATCH
            int count = in.readInt();
            List<Message> messages = new ArrayList<>(count);
            
            for (int i = 0; i < count; i++) {
                long offset = in.readLong();
                long timestamp = in.readLong();
                int size = in.readInt();
                byte[] payload = new byte[size];
                in.readFully(payload);
                
                messages.add(new Message(offset, timestamp, payload));
            }
            
            return messages;
        } else if (cmd == (byte) 0xFF) { // ERROR
            int code = in.readUnsignedShort();
            int msgLen = in.readUnsignedShort();
            byte[] msgBytes = new byte[msgLen];
            in.readFully(msgBytes);
            throw new BrokerException(code, new String(msgBytes, StandardCharsets.UTF_8));
        } else {
            throw new IOException("Unexpected response: 0x" + Integer.toHexString(cmd & 0xFF));
        }
    }
    
    @Override
    public void close() throws IOException {
        socket.close();
    }
}
```

### Message Record

```java
package minibroker.client;

import java.nio.charset.StandardCharsets;

/**
 * Represents a message received from the broker.
 */
public record Message(long offset, long timestamp, byte[] payload) {
    
    /**
     * Get payload as UTF-8 string.
     */
    public String payloadAsString() {
        return new String(payload, StandardCharsets.UTF_8);
    }
}
```

### Consumer with Offset Tracking

```java
package minibroker.client;

import java.io.*;
import java.nio.file.*;
import java.util.List;
import java.util.function.Consumer;

/**
 * Consumer that automatically tracks offsets.
 */
public class TrackedConsumer implements AutoCloseable {
    
    private final MiniBrokerConsumer consumer;
    private final Path offsetFile;
    private final String topic;
    private long currentOffset;
    
    public TrackedConsumer(String host, int port, String topic, Path offsetDir) throws IOException {
        this.consumer = new MiniBrokerConsumer(host, port);
        this.topic = topic;
        this.offsetFile = offsetDir.resolve(topic + ".offset");
        this.currentOffset = loadOffset();
    }
    
    /**
     * Poll for new messages and process them.
     * 
     * @param handler Function to process each message
     * @param batchSize Maximum messages to fetch per poll
     * @return Number of messages processed
     */
    public int poll(Consumer<Message> handler, int batchSize) throws IOException, BrokerException {
        List<Message> messages = consumer.fetch(topic, currentOffset, batchSize);
        
        for (Message msg : messages) {
            handler.accept(msg);
            currentOffset = msg.offset() + 1;
        }
        
        if (!messages.isEmpty()) {
            saveOffset();
        }
        
        return messages.size();
    }
    
    private long loadOffset() {
        try {
            if (Files.exists(offsetFile)) {
                String content = Files.readString(offsetFile).trim();
                return Long.parseLong(content);
            }
        } catch (Exception e) {
            System.err.println("Failed to load offset, starting from 0: " + e.getMessage());
        }
        return 0;
    }
    
    private void saveOffset() throws IOException {
        Files.createDirectories(offsetFile.getParent());
        Files.writeString(offsetFile, String.valueOf(currentOffset));
    }
    
    public long getCurrentOffset() {
        return currentOffset;
    }
    
    @Override
    public void close() throws IOException {
        consumer.close();
    }
}
```

---

## 4. Push Consumer Client

### MiniBrokerSubscriber.java

```java
package minibroker.client;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

/**
 * Push-based subscriber for MiniBroker.
 * 
 * Usage:
 * <pre>
 * var subscriber = new MiniBrokerSubscriber("localhost", 9092);
 * subscriber.subscribe("orders", msg -> {
 *     System.out.println("Received: " + msg.payloadAsString());
 * });
 * // Blocks until error or close() called
 * </pre>
 */
public class MiniBrokerSubscriber implements AutoCloseable {
    
    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;
    private volatile boolean running = true;
    
    public MiniBrokerSubscriber(String host, int port) throws IOException {
        this.socket = new Socket(host, port);
        this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
    }
    
    /**
     * Subscribe to a topic and receive messages via callback.
     * This method blocks until an error occurs or the subscriber is closed.
     * 
     * @param topic The topic to subscribe to
     * @param handler Callback for each received message
     */
    public void subscribe(String topic, Consumer<Message> handler) throws IOException {
        // Send SUBSCRIBE request
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        int payloadLen = 2 + topicBytes.length;
        
        out.writeInt(1 + payloadLen);
        out.writeByte(0x02); // SUBSCRIBE
        out.writeShort(topicBytes.length);
        out.write(topicBytes);
        out.flush();
        
        // Read messages as they arrive
        while (running) {
            try {
                int respLen = in.readInt();
                byte cmd = in.readByte();
                
                if (cmd == (byte) 0x82) { // MSG
                    long offset = in.readLong();
                    long timestamp = in.readLong();
                    int size = in.readInt();
                    byte[] payload = new byte[size];
                    in.readFully(payload);
                    
                    handler.accept(new Message(offset, timestamp, payload));
                } else if (cmd == (byte) 0xFF) { // ERROR
                    int code = in.readUnsignedShort();
                    int msgLen = in.readUnsignedShort();
                    byte[] msgBytes = new byte[msgLen];
                    in.readFully(msgBytes);
                    throw new BrokerException(code, new String(msgBytes, StandardCharsets.UTF_8));
                }
            } catch (EOFException e) {
                // Connection closed
                break;
            }
        }
    }
    
    /**
     * Subscribe in a background thread.
     * 
     * @return Thread that is running the subscription
     */
    public Thread subscribeAsync(String topic, Consumer<Message> handler) {
        Thread thread = Thread.ofVirtual().start(() -> {
            try {
                subscribe(topic, handler);
            } catch (IOException e) {
                if (running) {
                    System.err.println("Subscription error: " + e.getMessage());
                }
            }
        });
        return thread;
    }
    
    @Override
    public void close() throws IOException {
        running = false;
        socket.close();
    }
}
```

---

## 5. Connection Management

### Connection Pool

```java
package minibroker.client;

import java.io.IOException;
import java.util.concurrent.*;

/**
 * Connection pool for producer connections.
 */
public class ProducerPool implements AutoCloseable {
    
    private final String host;
    private final int port;
    private final BlockingQueue<MiniBrokerProducer> pool;
    private final int maxSize;
    
    public ProducerPool(String host, int port, int maxSize) {
        this.host = host;
        this.port = port;
        this.maxSize = maxSize;
        this.pool = new LinkedBlockingQueue<>(maxSize);
    }
    
    /**
     * Borrow a producer from the pool.
     */
    public MiniBrokerProducer borrow() throws IOException {
        MiniBrokerProducer producer = pool.poll();
        if (producer != null) {
            return producer;
        }
        return new MiniBrokerProducer(host, port);
    }
    
    /**
     * Return a producer to the pool.
     */
    public void release(MiniBrokerProducer producer) {
        if (!pool.offer(producer)) {
            try {
                producer.close();
            } catch (IOException e) {
                // Ignore
            }
        }
    }
    
    /**
     * Execute a publish with automatic borrow/release.
     */
    public long publish(String topic, byte[] data) throws IOException, BrokerException {
        MiniBrokerProducer producer = borrow();
        try {
            return producer.publish(topic, data);
        } finally {
            release(producer);
        }
    }
    
    @Override
    public void close() {
        MiniBrokerProducer producer;
        while ((producer = pool.poll()) != null) {
            try {
                producer.close();
            } catch (IOException e) {
                // Ignore
            }
        }
    }
}
```

### Reconnecting Client Wrapper

```java
package minibroker.client;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Producer that automatically reconnects on failure.
 */
public class ReconnectingProducer implements AutoCloseable {
    
    private final String host;
    private final int port;
    private final int maxRetries;
    private final long retryDelayMs;
    
    private MiniBrokerProducer producer;
    
    public ReconnectingProducer(String host, int port, int maxRetries, long retryDelayMs) 
            throws IOException {
        this.host = host;
        this.port = port;
        this.maxRetries = maxRetries;
        this.retryDelayMs = retryDelayMs;
        connect();
    }
    
    private void connect() throws IOException {
        this.producer = new MiniBrokerProducer(host, port);
    }
    
    public long publish(String topic, byte[] data) throws IOException, BrokerException {
        int attempts = 0;
        
        while (true) {
            try {
                return producer.publish(topic, data);
            } catch (IOException e) {
                attempts++;
                if (attempts >= maxRetries) {
                    throw e;
                }
                
                System.err.println("Publish failed, reconnecting (attempt " + attempts + ")");
                
                try {
                    producer.close();
                } catch (IOException ignored) {}
                
                try {
                    TimeUnit.MILLISECONDS.sleep(retryDelayMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted during reconnect", ie);
                }
                
                try {
                    connect();
                } catch (IOException ce) {
                    System.err.println("Reconnect failed: " + ce.getMessage());
                }
            }
        }
    }
    
    @Override
    public void close() throws IOException {
        if (producer != null) {
            producer.close();
        }
    }
}
```

---

## 6. Error Handling

### BrokerException.java

```java
package minibroker.client;

/**
 * Exception thrown when broker returns an error response.
 */
public class BrokerException extends Exception {
    
    public static final int TOPIC_NOT_FOUND = 0x0001;
    public static final int INVALID_OFFSET = 0x0002;
    public static final int INVALID_REQUEST = 0x0003;
    public static final int INTERNAL_ERROR = 0x0004;
    
    private final int errorCode;
    
    public BrokerException(int errorCode, String message) {
        super(String.format("[Error %04X] %s", errorCode, message));
        this.errorCode = errorCode;
    }
    
    public int getErrorCode() {
        return errorCode;
    }
    
    public boolean isRetryable() {
        return errorCode == INTERNAL_ERROR;
    }
    
    public boolean isTopicNotFound() {
        return errorCode == TOPIC_NOT_FOUND;
    }
}
```

---

## 7. Best Practices

### Producer Best Practices

```java
// 1. Reuse producer connections
// BAD - creates connection per message
for (String msg : messages) {
    try (var producer = new MiniBrokerProducer(host, port)) {
        producer.publish(topic, msg.getBytes());
    }
}

// GOOD - reuse connection
try (var producer = new MiniBrokerProducer(host, port)) {
    for (String msg : messages) {
        producer.publish(topic, msg.getBytes());
    }
}

// 2. Use batching for high throughput
try (var producer = new BatchingProducer(host, port, 100, 10)) {
    for (String msg : messages) {
        producer.publishAsync(topic, msg.getBytes());
    }
}

// 3. Handle errors appropriately
try {
    producer.publish(topic, data);
} catch (BrokerException e) {
    if (e.isRetryable()) {
        // Retry with backoff
    } else {
        // Log and skip
    }
} catch (IOException e) {
    // Network error - reconnect
}
```

### Consumer Best Practices

```java
// 1. Always track offsets
long offset = loadOffsetFromStorage();
while (running) {
    List<Message> messages = consumer.fetch(topic, offset, 100);
    for (Message msg : messages) {
        process(msg);
        offset = msg.offset() + 1;
    }
    saveOffsetToStorage(offset); // Commit after processing
}

// 2. Process-then-commit (at-least-once)
for (Message msg : messages) {
    process(msg);        // First process
    commitOffset(msg);   // Then commit
}
// If crash between process and commit: message reprocessed (duplicate)

// 3. Commit-then-process (at-most-once)
for (Message msg : messages) {
    commitOffset(msg);   // First commit
    process(msg);        // Then process
}
// If crash between commit and process: message lost

// 4. Use poll timeout to avoid busy-waiting
while (running) {
    List<Message> messages = consumer.fetch(topic, offset, 100);
    if (messages.isEmpty()) {
        Thread.sleep(100); // Wait before next poll
    }
}
```

### Subscriber Best Practices

```java
// 1. Handle messages quickly - don't block the callback
subscriber.subscribe("orders", msg -> {
    // GOOD - quick dispatch
    executor.submit(() -> processMessage(msg));
    
    // BAD - slow processing blocks receiving
    // processMessageSlowly(msg);
});

// 2. Use async subscription for non-blocking main thread
Thread subThread = subscriber.subscribeAsync("orders", this::handleMessage);
// ... main thread continues
subThread.join(); // Wait when done

// 3. Graceful shutdown
Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    subscriber.close();
}));
```
