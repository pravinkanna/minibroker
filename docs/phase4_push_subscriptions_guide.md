# MiniBroker Builder's Guide: Phase 4 – Push Subscriptions & Concurrency Deep-Dive

This guide covers **push-based subscriptions** and essential Java concurrency patterns.

---

## 1. Push Subscription Flow

```
┌──────────┐                ┌──────────┐                ┌──────────┐
│ Producer │                │  Broker  │                │Subscriber│
└────┬─────┘                └────┬─────┘                └────┬─────┘
     │                           │                           │
     │                           │    SUBSCRIBE("orders")    │
     │                           │<──────────────────────────│
     │                           │    (connection held open) │
     │                           │                           │
     │   PUBLISH("orders", msg)  │                           │
     │──────────────────────────>│                           │
     │                           │                           │
     │   ACK(offset=0)           │    MSG(offset=0, msg)     │
     │<──────────────────────────│──────────────────────────>│
     │                           │                           │
     │   PUBLISH("orders", msg2) │                           │
     │──────────────────────────>│                           │
     │                           │                           │
     │   ACK(offset=1)           │    MSG(offset=1, msg2)    │
     │<──────────────────────────│──────────────────────────>│
```

---

## 2. The Subscriber Problem

When a client sends `SUBSCRIBE`, we need to:
1. Add them to the topic's subscriber list
2. Keep their connection open indefinitely
3. Push new messages as they arrive
4. Remove them when they disconnect

### Key Insight

The **subscriber's virtual thread blocks forever**. It doesn't return from `handleSubscribe()`. Meanwhile, **publisher threads** call `topic.broadcast()` which writes to the subscriber's socket.

This is why we use `CopyOnWriteArrayList`:
- Publishers iterate without locking
- Subscriber add/remove is rare
- Thread-safe iteration during modification

---

## 3. Enhanced Topic with Subscriber State

```java
package minibroker.topic;

import minibroker.log.LogEntry;
import minibroker.log.TopicLog;
import minibroker.protocol.ProtocolWriter;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;

public class Topic implements Closeable {
    
    private final String name;
    private final TopicLog log;
    private final CopyOnWriteArrayList<Subscriber> subscribers = new CopyOnWriteArrayList<>();
    
    public Topic(String name, TopicLog log) {
        this.name = name;
        this.log = log;
    }
    
    public long publish(byte[] data) throws IOException {
        long offset = log.append(data);
        LogEntry entry = log.read(offset);
        broadcast(entry);
        return offset;
    }
    
    private void broadcast(LogEntry entry) {
        for (Subscriber sub : subscribers) {
            try {
                sub.writer().writeMsg(entry);
            } catch (IOException e) {
                // Mark for removal (disconnect detected)
                sub.markDisconnected();
            }
        }
        // Cleanup disconnected subscribers
        subscribers.removeIf(Subscriber::isDisconnected);
    }
    
    public void addSubscriber(ProtocolWriter writer, String clientId) {
        subscribers.add(new Subscriber(clientId, writer));
        System.out.println("Subscriber added: " + clientId + " to topic: " + name);
    }
    
    public void removeSubscriber(ProtocolWriter writer) {
        subscribers.removeIf(s -> s.writer() == writer);
    }
    
    public LogEntry read(long offset) throws IOException {
        return log.read(offset);
    }
    
    public long size() {
        return log.size();
    }
    
    @Override
    public void close() throws IOException {
        log.close();
    }
    
    // Inner class for subscriber state
    private static class Subscriber {
        private final String clientId;
        private final ProtocolWriter writer;
        private volatile boolean disconnected = false;
        
        Subscriber(String clientId, ProtocolWriter writer) {
            this.clientId = clientId;
            this.writer = writer;
        }
        
        ProtocolWriter writer() { return writer; }
        void markDisconnected() { disconnected = true; }
        boolean isDisconnected() { return disconnected; }
    }
}
```

---

## 4. Updated ClientHandler Subscribe

```java
private void handleSubscribe(SubscribeRequest req, ProtocolWriter writer) throws IOException {
    Topic topic = registry.getOrCreate(req.topic());
    String clientId = socket.getRemoteSocketAddress().toString();
    
    topic.addSubscriber(writer, clientId);
    
    try {
        // This virtual thread now "parks" here forever
        // The socket stays open, and broadcast() writes to it
        while (!socket.isClosed()) {
            Thread.sleep(1000);
        }
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
    } finally {
        topic.removeSubscriber(writer);
        System.out.println("Subscriber disconnected: " + clientId);
    }
}
```

---

## 5. Java Concurrency Patterns Used

### Pattern 1: CopyOnWriteArrayList

**When to use**: Many reads, few writes.

```java
// Every write creates a new array copy
List<T> list = new CopyOnWriteArrayList<>();

// Iteration is snapshot-based - safe even if list modified
for (T item : list) {
    // No ConcurrentModificationException
}
```

### Pattern 2: ConcurrentHashMap.computeIfAbsent

**When to use**: Lazy initialization of map values.

```java
ConcurrentHashMap<String, Topic> topics = new ConcurrentHashMap<>();

// Thread-safe: only one thread creates the Topic
Topic topic = topics.computeIfAbsent("orders", name -> {
    return new Topic(name, new TopicLog(...));
});
```

### Pattern 3: ReentrantLock for Write Serialization

**When to use**: Multiple threads writing to same resource.

```java
private final ReentrantLock writeLock = new ReentrantLock();

public long append(byte[] data) throws IOException {
    writeLock.lock();
    try {
        // Only one thread at a time can append
        return doAppend(data);
    } finally {
        writeLock.unlock();
    }
}
```

### Pattern 4: Volatile for Visibility

**When to use**: Flag checked across threads.

```java
private volatile boolean running = true;

// Thread 1: Server loop
while (running) {
    accept();
}

// Thread 2: Shutdown hook
running = false;
```

---

## 6. Test Push Subscriber Client

```java
package minibroker.client;

import java.io.*;
import java.net.Socket;

public class TestSubscriber {
    
    public static void main(String[] args) throws Exception {
        try (Socket socket = new Socket("localhost", 9092);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {
            
            String topic = "test-topic";
            
            // Send SUBSCRIBE
            int payloadLen = 2 + topic.length();
            out.writeInt(1 + payloadLen);
            out.writeByte(0x02); // SUBSCRIBE
            out.writeShort(topic.length());
            out.write(topic.getBytes());
            out.flush();
            
            System.out.println("Subscribed to: " + topic);
            System.out.println("Waiting for messages (Ctrl+C to exit)...");
            
            // Read messages forever
            while (true) {
                int respLen = in.readInt();
                byte cmd = in.readByte();
                
                if (cmd == (byte) 0x82) { // MSG
                    long offset = in.readLong();
                    long timestamp = in.readLong();
                    int size = in.readInt();
                    byte[] payload = new byte[size];
                    in.readFully(payload);
                    
                    System.out.printf("[%d] %s%n", offset, new String(payload));
                }
            }
        }
    }
}
```

---

## 7. Testing Push Flow

```bash
# Terminal 1: Broker
mvn compile exec:java -Dexec.mainClass="minibroker.Main"

# Terminal 2: Subscriber (blocks, waiting for messages)
mvn compile exec:java -Dexec.mainClass="minibroker.client.TestSubscriber"

# Terminal 3: Producer (publishes messages)
mvn compile exec:java -Dexec.mainClass="minibroker.client.TestProducer"
# Run multiple times - see messages appear in Terminal 2!
```

---

## 8. Debugging Concurrency Issues

### Issue 1: Messages Not Delivered

**Symptom**: Publisher gets ACK, but subscriber sees nothing.

**Debug**:
```java
private void broadcast(LogEntry entry) {
    System.out.println("Broadcasting to " + subscribers.size() + " subscribers");
    for (Subscriber sub : subscribers) {
        System.out.println("  -> " + sub.clientId);
        // ...
    }
}
```

### Issue 2: ConcurrentModificationException

**Symptom**: Exception during iteration.

**Cause**: Using `ArrayList` instead of `CopyOnWriteArrayList`.

### Issue 3: Subscriber Never Added

**Symptom**: Subscriber count is 0.

**Debug**: Check that `addSubscriber` is called BEFORE the blocking loop.

---

## 9. Performance Considerations

| Aspect | Current Design | Optimization |
| :--- | :--- | :--- |
| Write lock | Per-topic lock | Lock-free append (complex) |
| Broadcast | Synchronous | Async with queue |
| Subscriber list | CopyOnWriteArrayList | Lock-striped map |
| File writes | Immediate | Batched flush |

For this learning project, **simplicity wins**. The current design handles hundreds of concurrent clients easily.

---

Ready for **Phase 5: Recovery & Polish**!
