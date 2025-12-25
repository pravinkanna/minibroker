# MiniBroker Builder's Guide: Phase 3 – The Broker Server

This guide covers building the **TCP server** that accepts connections and processes commands.

---

## 1. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         BrokerServer                            │
│  ┌─────────────┐                                                │
│  │ ServerSocket│ ──accept()──> Virtual Thread per client        │
│  └─────────────┘               │                                │
│                                ▼                                │
│                         ┌──────────────┐                        │
│                         │ClientHandler │                        │
│                         │  - reader    │                        │
│                         │  - writer    │                        │
│                         │  - registry  │                        │
│                         └──────────────┘                        │
│                                │                                │
│                                ▼                                │
│                         ┌──────────────┐                        │
│                         │TopicRegistry │──> Topic ──> TopicLog  │
│                         └──────────────┘                        │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. Why Virtual Threads?

Traditional approaches:
- **Thread-per-client**: Simple but doesn't scale (OS threads are expensive)
- **NIO/Selectors**: Scales but complex (callback hell)
- **Netty**: Abstracts NIO but adds dependency

**Virtual Threads (Java 21+)**:
- Simple blocking code (like thread-per-client)
- Scales to millions of connections
- JVM handles the complexity

```java
// Old way - limited to ~10K threads
ExecutorService executor = Executors.newFixedThreadPool(200);

// New way - millions of virtual threads
ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
```

---

## 3. TopicRegistry

Central registry of all topics.

```java
package minibroker.topic;

import minibroker.log.TopicLog;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;

public class TopicRegistry {
    
    private final Path dataDir;
    private final ConcurrentHashMap<String, Topic> topics = new ConcurrentHashMap<>();
    
    public TopicRegistry(Path dataDir) {
        this.dataDir = dataDir;
    }
    
    public Topic getOrCreate(String name) throws IOException {
        return topics.computeIfAbsent(name, n -> {
            try {
                Path topicDir = dataDir.resolve(n);
                TopicLog log = new TopicLog(topicDir);
                return new Topic(n, log);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create topic: " + n, e);
            }
        });
    }
    
    public Topic get(String name) {
        return topics.get(name);
    }
    
    public void closeAll() throws IOException {
        for (Topic topic : topics.values()) {
            topic.close();
        }
    }
}
```

---

## 4. Topic

Wraps a TopicLog and manages subscribers.

```java
package minibroker.topic;

import minibroker.log.LogEntry;
import minibroker.log.TopicLog;
import minibroker.protocol.ProtocolWriter;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class Topic implements Closeable {
    
    private final String name;
    private final TopicLog log;
    private final List<ProtocolWriter> subscribers = new CopyOnWriteArrayList<>();
    
    public Topic(String name, TopicLog log) {
        this.name = name;
        this.log = log;
    }
    
    public String name() { return name; }
    
    public long publish(byte[] data) throws IOException {
        long offset = log.append(data);
        
        // Broadcast to push subscribers
        LogEntry entry = log.read(offset);
        broadcast(entry);
        
        return offset;
    }
    
    public LogEntry read(long offset) throws IOException {
        return log.read(offset);
    }
    
    public long size() {
        return log.size();
    }
    
    public void addSubscriber(ProtocolWriter writer) {
        subscribers.add(writer);
    }
    
    public void removeSubscriber(ProtocolWriter writer) {
        subscribers.remove(writer);
    }
    
    private void broadcast(LogEntry entry) {
        for (ProtocolWriter writer : subscribers) {
            try {
                writer.writeMsg(entry);
            } catch (IOException e) {
                // Client disconnected
                removeSubscriber(writer);
            }
        }
    }
    
    @Override
    public void close() throws IOException {
        log.close();
    }
}
```

---

## 5. BrokerServer

The main server class.

```java
package minibroker.server;

import minibroker.topic.TopicRegistry;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BrokerServer {
    
    private final int port;
    private final TopicRegistry registry;
    private volatile boolean running = true;
    private ServerSocket serverSocket;
    
    public BrokerServer(int port, TopicRegistry registry) {
        this.port = port;
        this.registry = registry;
    }
    
    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        System.out.println("MiniBroker listening on port " + port);
        
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            while (running) {
                try {
                    Socket client = serverSocket.accept();
                    System.out.println("Client connected: " + client.getRemoteSocketAddress());
                    executor.submit(new ClientHandler(client, registry));
                } catch (IOException e) {
                    if (running) {
                        System.err.println("Accept error: " + e.getMessage());
                    }
                }
            }
        }
    }
    
    public void stop() {
        running = false;
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException e) {
            // Ignore
        }
    }
}
```

---

## 6. ClientHandler

Handles one client connection.

```java
package minibroker.server;

import minibroker.log.LogEntry;
import minibroker.protocol.*;
import minibroker.topic.Topic;
import minibroker.topic.TopicRegistry;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class ClientHandler implements Runnable {
    
    private final Socket socket;
    private final TopicRegistry registry;
    
    public ClientHandler(Socket socket, TopicRegistry registry) {
        this.socket = socket;
        this.registry = registry;
    }
    
    @Override
    public void run() {
        try (socket;
             var reader = new ProtocolReader(socket.getInputStream());
             var writer = new ProtocolWriter(socket.getOutputStream())) {
            
            while (true) {
                Request request = reader.readRequest();
                if (request == null) break; // Client disconnected
                
                try {
                    handleRequest(request, writer);
                } catch (Exception e) {
                    writer.writeError(0x0004, "Internal error: " + e.getMessage());
                }
            }
            
        } catch (IOException e) {
            System.out.println("Client disconnected: " + e.getMessage());
        }
    }
    
    private void handleRequest(Request request, ProtocolWriter writer) throws IOException {
        switch (request) {
            case PublishRequest pub -> handlePublish(pub, writer);
            case FetchRequest fetch -> handleFetch(fetch, writer);
            case SubscribeRequest sub -> handleSubscribe(sub, writer);
            case CreateTopicRequest create -> handleCreateTopic(create, writer);
        }
    }
    
    private void handlePublish(PublishRequest req, ProtocolWriter writer) throws IOException {
        Topic topic = registry.getOrCreate(req.topic());
        long offset = topic.publish(req.data());
        writer.writeAck(offset);
    }
    
    private void handleFetch(FetchRequest req, ProtocolWriter writer) throws IOException {
        Topic topic = registry.get(req.topic());
        if (topic == null) {
            writer.writeError(0x0001, "Topic not found: " + req.topic());
            return;
        }
        
        List<LogEntry> entries = new ArrayList<>();
        long offset = req.startOffset();
        int count = 0;
        
        while (count < req.maxCount() && offset < topic.size()) {
            entries.add(topic.read(offset));
            offset++;
            count++;
        }
        
        writer.writeBatch(entries);
    }
    
    private void handleSubscribe(SubscribeRequest req, ProtocolWriter writer) throws IOException {
        Topic topic = registry.getOrCreate(req.topic());
        topic.addSubscriber(writer);
        
        try {
            // Block forever - messages are pushed via broadcast()
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            topic.removeSubscriber(writer);
        }
    }
    
    private void handleCreateTopic(CreateTopicRequest req, ProtocolWriter writer) throws IOException {
        registry.getOrCreate(req.topic());
        writer.writeAck(0);
    }
}
```

---

## 7. Main Entry Point

```java
package minibroker;

import minibroker.server.BrokerServer;
import minibroker.topic.TopicRegistry;
import java.nio.file.Path;

public class Main {
    
    public static void main(String[] args) throws Exception {
        int port = 9092;
        Path dataDir = Path.of("./data");
        
        TopicRegistry registry = new TopicRegistry(dataDir);
        BrokerServer server = new BrokerServer(port, registry);
        
        // Graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            server.stop();
            try {
                registry.closeAll();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));
        
        server.start();
    }
}
```

---

## 8. Test Client (Producer)

```java
package minibroker.client;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class TestProducer {
    
    public static void main(String[] args) throws Exception {
        try (Socket socket = new Socket("localhost", 9092);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {
            
            String topic = "test-topic";
            byte[] data = "Hello, MiniBroker!".getBytes(StandardCharsets.UTF_8);
            
            // Build PUBLISH request
            int payloadLen = 2 + topic.length() + 4 + data.length;
            out.writeInt(1 + payloadLen);
            out.writeByte(0x01); // PUBLISH
            out.writeShort(topic.length());
            out.write(topic.getBytes());
            out.writeInt(data.length);
            out.write(data);
            out.flush();
            
            // Read ACK
            int respLen = in.readInt();
            byte cmd = in.readByte();
            long offset = in.readLong();
            
            System.out.println("Published at offset: " + offset);
        }
    }
}
```

---

## 9. Test Client (Consumer)

```java
package minibroker.client;

import java.io.*;
import java.net.Socket;

public class TestConsumer {
    
    public static void main(String[] args) throws Exception {
        try (Socket socket = new Socket("localhost", 9092);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {
            
            String topic = "test-topic";
            
            // Build FETCH request
            int payloadLen = 2 + topic.length() + 8 + 4;
            out.writeInt(1 + payloadLen);
            out.writeByte(0x03); // FETCH
            out.writeShort(topic.length());
            out.write(topic.getBytes());
            out.writeLong(0);  // startOffset
            out.writeInt(100); // maxCount
            out.flush();
            
            // Read BATCH
            int respLen = in.readInt();
            byte cmd = in.readByte();
            int count = in.readInt();
            
            System.out.println("Received " + count + " messages:");
            for (int i = 0; i < count; i++) {
                long offset = in.readLong();
                long timestamp = in.readLong();
                int size = in.readInt();
                byte[] payload = new byte[size];
                in.readFully(payload);
                System.out.println("  [" + offset + "] " + new String(payload));
            }
        }
    }
}
```

---

## 10. Run & Test

```bash
# Terminal 1: Start broker
mvn compile exec:java -Dexec.mainClass="minibroker.Main"

# Terminal 2: Produce
mvn compile exec:java -Dexec.mainClass="minibroker.client.TestProducer"

# Terminal 3: Consume
mvn compile exec:java -Dexec.mainClass="minibroker.client.TestConsumer"
```

---

Ready for **Phase 4: Push Subscriptions & Polish**!
