# MiniBroker Builder's Guide: Phase 2 – The Wire Protocol

This guide covers the **binary protocol** that clients use to communicate with the broker over TCP.

---

## 1. Protocol Design Philosophy

### Why Binary Instead of Text (HTTP/JSON)?

| Factor | Binary | Text (HTTP/JSON) |
| :--- | :--- | :--- |
| Parsing speed | Fast (direct byte reads) | Slow (string parsing) |
| Message size | Compact | Verbose |
| Streaming | Natural fit | Awkward |
| Learning value | High (byte manipulation) | Lower |

---

## 2. Packet Structure

Every packet follows this structure:

```
┌────────────────┬─────────────┬─────────────────────┐
│  Length (4B)   │ Command (1B)│  Payload (variable) │
└────────────────┴─────────────┴─────────────────────┘
```

- **Length**: Total packet size (excluding the length field itself)
- **Command**: Operation type
- **Payload**: Command-specific data

---

## 3. Command Definitions

### Request Commands (Client → Broker)

| Byte | Name | Payload |
| :--- | :--- | :--- |
| `0x01` | PUBLISH | `[TopicLen(2)][Topic][DataLen(4)][Data]` |
| `0x02` | SUBSCRIBE | `[TopicLen(2)][Topic]` |
| `0x03` | FETCH | `[TopicLen(2)][Topic][Offset(8)][MaxCount(4)]` |
| `0x04` | CREATE_TOPIC | `[TopicLen(2)][Topic]` |

### Response Commands (Broker → Client)

| Byte | Name | Payload |
| :--- | :--- | :--- |
| `0x81` | ACK | `[Offset(8)]` |
| `0x82` | MSG | `[Offset(8)][Timestamp(8)][DataLen(4)][Data]` |
| `0x83` | BATCH | `[Count(4)][MSG...]` |
| `0xFF` | ERROR | `[Code(2)][MsgLen(2)][Msg]` |

---

## 4. Implementing Command Enum

```java
package minibroker.protocol;

public enum Command {
    PUBLISH((byte) 0x01),
    SUBSCRIBE((byte) 0x02),
    FETCH((byte) 0x03),
    CREATE_TOPIC((byte) 0x04),
    ACK((byte) 0x81),
    MSG((byte) 0x82),
    BATCH((byte) 0x83),
    ERROR((byte) 0xFF);
    
    private final byte code;
    
    Command(byte code) { this.code = code; }
    
    public byte code() { return code; }
    
    public static Command fromByte(byte b) {
        for (Command cmd : values()) {
            if (cmd.code == b) return cmd;
        }
        throw new IllegalArgumentException("Unknown: 0x" + Integer.toHexString(b & 0xFF));
    }
}
```

---

## 5. ProtocolReader

```java
package minibroker.protocol;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class ProtocolReader {
    private final DataInputStream in;
    
    public ProtocolReader(InputStream inputStream) {
        this.in = new DataInputStream(new BufferedInputStream(inputStream));
    }
    
    public Request readRequest() throws IOException {
        int length;
        try {
            length = in.readInt();
        } catch (EOFException e) {
            return null;
        }
        
        if (length < 1 || length > 100_000_000) {
            throw new ProtocolException("Invalid frame length: " + length);
        }
        
        byte cmdByte = in.readByte();
        Command command = Command.fromByte(cmdByte);
        
        byte[] payload = new byte[length - 1];
        in.readFully(payload);
        
        return parseRequest(command, payload);
    }
    
    private Request parseRequest(Command cmd, byte[] payload) throws IOException {
        var payloadIn = new DataInputStream(new ByteArrayInputStream(payload));
        return switch (cmd) {
            case PUBLISH -> new PublishRequest(readString(payloadIn), readBytes(payloadIn));
            case SUBSCRIBE -> new SubscribeRequest(readString(payloadIn));
            case FETCH -> new FetchRequest(readString(payloadIn), payloadIn.readLong(), payloadIn.readInt());
            case CREATE_TOPIC -> new CreateTopicRequest(readString(payloadIn));
            default -> throw new ProtocolException("Unexpected: " + cmd);
        };
    }
    
    private String readString(DataInputStream in) throws IOException {
        int len = in.readUnsignedShort();
        byte[] bytes = new byte[len];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
    
    private byte[] readBytes(DataInputStream in) throws IOException {
        int len = in.readInt();
        byte[] bytes = new byte[len];
        in.readFully(bytes);
        return bytes;
    }
}
```

---

## 6. ProtocolWriter

```java
package minibroker.protocol;

import minibroker.log.LogEntry;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ProtocolWriter {
    private final DataOutputStream out;
    
    public ProtocolWriter(OutputStream outputStream) {
        this.out = new DataOutputStream(new BufferedOutputStream(outputStream));
    }
    
    public void writeAck(long offset) throws IOException {
        out.writeInt(9); // 1 + 8
        out.writeByte(Command.ACK.code());
        out.writeLong(offset);
        out.flush();
    }
    
    public void writeMsg(LogEntry entry) throws IOException {
        byte[] payload = entry.payload();
        int length = 1 + 8 + 8 + 4 + payload.length;
        out.writeInt(length);
        out.writeByte(Command.MSG.code());
        out.writeLong(entry.offset());
        out.writeLong(entry.timestamp());
        out.writeInt(payload.length);
        out.write(payload);
        out.flush();
    }
    
    public void writeBatch(List<LogEntry> entries) throws IOException {
        var buffer = new ByteArrayOutputStream();
        var bufOut = new DataOutputStream(buffer);
        bufOut.writeInt(entries.size());
        for (LogEntry e : entries) {
            byte[] p = e.payload();
            bufOut.writeLong(e.offset());
            bufOut.writeLong(e.timestamp());
            bufOut.writeInt(p.length);
            bufOut.write(p);
        }
        bufOut.flush();
        byte[] data = buffer.toByteArray();
        out.writeInt(1 + data.length);
        out.writeByte(Command.BATCH.code());
        out.write(data);
        out.flush();
    }
    
    public void writeError(int code, String message) throws IOException {
        byte[] msgBytes = message.getBytes(StandardCharsets.UTF_8);
        out.writeInt(1 + 2 + 2 + msgBytes.length);
        out.writeByte(Command.ERROR.code());
        out.writeShort(code);
        out.writeShort(msgBytes.length);
        out.write(msgBytes);
        out.flush();
    }
}
```

---

## 7. Request Objects

```java
public sealed interface Request permits PublishRequest, SubscribeRequest, FetchRequest, CreateTopicRequest {
    Command command();
}

public record PublishRequest(String topic, byte[] data) implements Request {
    public Command command() { return Command.PUBLISH; }
}

public record SubscribeRequest(String topic) implements Request {
    public Command command() { return Command.SUBSCRIBE; }
}

public record FetchRequest(String topic, long startOffset, int maxCount) implements Request {
    public Command command() { return Command.FETCH; }
}

public record CreateTopicRequest(String topic) implements Request {
    public Command command() { return Command.CREATE_TOPIC; }
}
```

---

## 8. Common Mistakes

1. **Forgetting to flush** - Always call `out.flush()` after writing
2. **String encoding** - Always use `StandardCharsets.UTF_8`
3. **Signed vs unsigned** - Use `readUnsignedShort()` for lengths
4. **Partial reads** - Use `readFully()` instead of `read()`

---

Ready for **Phase 3: The Broker Server**!
