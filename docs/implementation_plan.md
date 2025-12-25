# MiniBroker: Implementation Action Plan

This document is your **step-by-step guide** to building "MiniBroker". Each phase builds on the last. Complete all tasks in a phase before moving on.

---

## Phase 0: Project Setup (Day 1)

### 0.1 Initialize Project
```bash
mkdir minibroker && cd minibroker
# Create a standard Maven/Gradle project structure
# We'll use Maven for simplicity
```

### 0.2 Project Structure
```
minibroker/
├── pom.xml
└── src/
    ├── main/java/minibroker/
    │   ├── Main.java                 # Entry point
    │   ├── config/
    │   │   └── BrokerConfig.java     # Data dir, port, flush policy
    │   ├── log/
    │   │   ├── LogEntry.java         # Record: offset, timestamp, payload
    │   │   ├── LogSegment.java       # Single file abstraction
    │   │   └── TopicLog.java         # Manages segments for a topic
    │   ├── protocol/
    │   │   ├── Command.java          # Enum: PUBLISH, FETCH, SUBSCRIBE...
    │   │   ├── ProtocolReader.java   # Parse bytes -> Request object
    │   │   └── ProtocolWriter.java   # Serialize Response -> bytes
    │   ├── server/
    │   │   ├── BrokerServer.java     # TCP Listener, accepts connections
    │   │   └── ClientHandler.java    # Handles one client connection
    │   └── topic/
    │       ├── Topic.java            # Represents a single topic
    │       └── TopicRegistry.java    # Map of all topics
    └── test/java/minibroker/
        └── log/
            └── TopicLogTest.java
```

### 0.3 `pom.xml` Essentials
```xml
<project>
    <modelVersion>4.0.0</modelVersion>
    <groupId>minibroker</groupId>
    <artifactId>minibroker</artifactId>
    <version>0.1.0-SNAPSHOT</version>
    <properties>
        <maven.compiler.source>21</maven.compiler.source>
        <maven.compiler.target>21</maven.compiler.target>
    </properties>
    <dependencies>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <version>5.10.1</version>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

> [!TIP]
> Run `java --version` to confirm you have JDK 21+. Virtual Threads are a preview feature in 19/20 but stable in 21.

---

## Phase 1: The Core Log Engine (Days 2-4)

This is the heart of the broker. Get this right before touching networking.

### Task 1.1: `LogEntry.java`
A simple immutable record.
```java
public record LogEntry(
    long offset,
    long timestamp,
    byte[] payload
) {}
```

### Task 1.2: `LogSegment.java`
Manages a single `.log` file on disk.

**Key Methods**:
| Method | Description |
| :--- | :--- |
| `LogSegment(Path path)` | Opens or creates the file. |
| `long append(byte[] data)` | Writes header + payload, returns offset. |
| `LogEntry read(long position)` | Reads one entry at a file position. |
| `void flush()` | Calls `FileChannel.force(true)`. |
| `long size()` | Current file size. |
| `void close()` | Closes the channel. |

**Implementation Notes**:
*   Use `FileChannel` opened in `StandardOpenOption.CREATE, APPEND, READ`.
*   The `append` operation:
    1.  Lock with `ReentrantLock`.
    2.  Calculate CRC32 of payload.
    3.  Write: `[CRC32 (4)] [Offset (8)] [Timestamp (8)] [Size (4)] [Payload]`.
    4.  Increment the internal offset counter.
    5.  Unlock.
*   The `read` operation takes a `filePosition` (byte offset in file), reads the header, validates CRC, returns `LogEntry`.

### Task 1.3: `TopicLog.java`
Higher-level abstraction over potentially multiple segments.

**Key Methods**:
| Method | Description |
| :--- | :--- |
| `TopicLog(Path topicDir)` | Loads existing segments or creates new. |
| `long append(byte[] data)` | Delegates to active segment, handles rollover (optional for v1). |
| `LogEntry read(long offset)` | Finds segment, translates offset to file position, reads. |
| `long getLatestOffset()` | Returns the next offset to be written. |

**Simplification for V1**: Use a single segment. Skip rollover logic.

### Task 1.4: Unit Tests
Write tests in `TopicLogTest.java`:
1.  `testAppendAndReadSingle()`: Append 1 message, read it back.
2.  `testAppendAndReadMultiple()`: Append 100 messages, read all back by offset.
3.  `testPersistence()`: Append, close, reopen, read.
4.  `testRecovery()`: Append, corrupt the last 5 bytes of the file (simulate crash), reopen, verify it truncates and recovers.

> [!IMPORTANT]
> **Milestone**: All Phase 1 tests pass. You have a working, persistent log.

---

## Phase 2: The Wire Protocol (Days 5-6)

### Task 2.1: `Command.java` (Enum)
```java
public enum Command {
    PUBLISH((byte) 0x01),
    SUBSCRIBE((byte) 0x02),
    FETCH((byte) 0x03),
    CREATE_TOPIC((byte) 0x04),
    // Responses
    ACK((byte) 0x81),
    MSG((byte) 0x82),
    BATCH((byte) 0x83),
    ERR((byte) 0xFF);

    public final byte code;
    Command(byte code) { this.code = code; }

    public static Command fromByte(byte b) { /* lookup */ }
}
```

### Task 2.2: `ProtocolReader.java`
Wraps a `DataInputStream`. Provides methods to parse incoming requests.
```java
public class ProtocolReader {
    private final DataInputStream in;

    public ProtocolReader(InputStream in) {
        this.in = new DataInputStream(in);
    }

    // Reads [Length(4)][Command(1)][Payload...] -> returns a Request object
    public Request readRequest() throws IOException { ... }
}
```

### Task 2.3: `ProtocolWriter.java`
Wraps a `DataOutputStream`. Provides methods to write responses.
```java
public class ProtocolWriter {
    private final DataOutputStream out;

    public void writeAck(long offset) throws IOException { ... }
    public void writeMsg(LogEntry entry) throws IOException { ... }
    public void writeError(int code, String message) throws IOException { ... }
}
```

### Task 2.4: Integration Test
Write a simple loopback test:
1.  Create `ByteArrayOutputStream`.
2.  Use `ProtocolWriter` to write a `PUBLISH` request.
3.  Use `ProtocolReader` to parse it.
4.  Assert the parsed request matches.

---

## Phase 3: The Broker Server & PUBLISH (Days 7-9)

### Task 3.1: `BrokerServer.java`
```java
public class BrokerServer {
    private final int port;
    private final TopicRegistry registry;
    private volatile boolean running = true;

    public void start() throws IOException {
        try (var serverSocket = new ServerSocket(port);
             var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            while (running) {
                Socket client = serverSocket.accept();
                executor.submit(() -> new ClientHandler(client, registry).run());
            }
        }
    }
}
```

### Task 3.2: `ClientHandler.java`
Handles one client connection in a virtual thread.
```java
public class ClientHandler implements Runnable {
    private final Socket socket;
    private final TopicRegistry registry;

    @Override
    public void run() {
        try (socket;
             var reader = new ProtocolReader(socket.getInputStream());
             var writer = new ProtocolWriter(socket.getOutputStream())) {
            while (true) {
                Request req = reader.readRequest();
                switch (req.command()) {
                    case PUBLISH -> handlePublish(req, writer);
                    case FETCH -> handleFetch(req, writer);
                    case SUBSCRIBE -> handleSubscribe(req, writer);
                    // ...
                }
            }
        } catch (IOException e) {
            // Client disconnected
        }
    }
}
```

### Task 3.3: Handle `PUBLISH`
1.  Get topic from `TopicRegistry`.
2.  Call `topic.getLog().append(payload)`.
3.  Get back `offset`.
4.  Call `writer.writeAck(offset)`.

### Task 3.4: End-to-End Test (Manual)
Write a simple `TestProducer.java` main class:
1.  Connect to `localhost:9092`.
2.  Send 5 `PUBLISH` commands.
3.  Print ACKs.
4.  Verify the `.log` file on disk contains data.

> [!IMPORTANT]
> **Milestone**: You can publish messages and see them persisted to disk.

---

## Phase 4: Pull Consumption (Days 10-11)

### Task 4.1: Handle `FETCH`
1.  Parse `offset` and `batchSize` from request.
2.  Loop: `log.read(offset)` -> `log.read(offset+1)` ... up to `batchSize`.
3.  Write `BATCH` response: `[Count(4)]` then `Count` x `MSG` frames.

### Task 4.2: `TestConsumer.java`
1.  Connect to broker.
2.  Send `FETCH(topic, offset=0, batch=10)`.
3.  Print received messages.

---

## Phase 5: Push Subscriptions (Days 12-14)

### Task 5.1: Subscriber Management in `Topic.java`
```java
public class Topic {
    private final TopicLog log;
    private final List<ProtocolWriter> subscribers = new CopyOnWriteArrayList<>();

    public void addSubscriber(ProtocolWriter writer) {
        subscribers.add(writer);
    }

    public void removeSubscriber(ProtocolWriter writer) {
        subscribers.remove(writer);
    }

    public void broadcast(LogEntry entry) {
        for (var writer : subscribers) {
            try {
                writer.writeMsg(entry);
            } catch (IOException e) {
                removeSubscriber(writer);
            }
        }
    }
}
```

### Task 5.2: Modify `handlePublish`
After `log.append()`, call `topic.broadcast(newEntry)`.

### Task 5.3: Handle `SUBSCRIBE`
1.  Get topic.
2.  Add `writer` to topic's subscriber list.
3.  **Block the virtual thread** (the connection stays open, the thread "sleeps" waiting for data).
4.  The `broadcast` method will write to this socket when new messages arrive.

> [!CAUTION]
> The subscribing thread must not exit the handler loop, or the socket closes. Consider a `while(true) { Thread.sleep(Long.MAX_VALUE); }` or use a `CountDownLatch` that never counts down. The actual "wake up and write" happens from the *publisher's* virtual thread calling `broadcast`.

### Task 5.4: Test Push
1.  Start broker.
2.  Run `TestConsumer` in SUBSCRIBE mode (prints messages as they arrive).
3.  Run `TestProducer` to publish.
4.  Verify `TestConsumer` prints messages immediately.

---

## Phase 6: Recovery & Polish (Days 15-17)

### Task 6.1: Startup Recovery in `LogSegment`
On `LogSegment(path)` constructor:
1.  Scan the file from the beginning.
2.  For each record, validate CRC.
3.  If CRC fails or read is incomplete, truncate file at that position.
4.  Set `nextOffset` based on last valid record.

### Task 6.2: Graceful Shutdown
*   Catch `SIGINT` in `Main.java`.
*   Call `server.stop()`.
*   Flush all logs.

### Task 6.3: CLI Arguments
Use a simple args parser or just `BrokerConfig` to read:
*   `--port 9092`
*   `--data-dir ./data`

---

## Suggested Daily Schedule

| Day | Focus | Deliverable |
| :--- | :--- | :--- |
| 1 | Project setup, Maven, structure | Empty project compiles |
| 2-3 | `LogSegment` implementation | Unit tests for single file append/read |
| 4 | `TopicLog`, recovery logic | Persistence tests pass |
| 5-6 | Wire protocol (`ProtocolReader/Writer`) | Loopback parsing test |
| 7-8 | `BrokerServer`, `ClientHandler`, PUBLISH | Manual producer test works |
| 9-10 | FETCH handler, Pull consumer | Pull consumer test works |
| 11-12 | SUBSCRIBE handler, Push broadcast | Push consumer test works |
| 13-14 | Recovery, graceful shutdown | Crash/restart test works |
| 15+ | Polish, refactor, README | Clean codebase |

---

## Stretch Goals (Post-MVP)

*   [ ] Log compaction (key-based retention).
*   [ ] Multiple segments with rollover.
*   [ ] Memory-mapped file reads for speed.
*   [ ] Simple authentication (token-based).
*   [ ] Metrics endpoint (expose via simple HTTP).
*   [ ] Consumer groups with offset tracking on broker.

---

Good luck, and happy building! 🚀
