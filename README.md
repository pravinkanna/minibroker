# MiniBroker

A simple and minimal message broker built from scratch in Java.

## What is this?

MiniBroker is a **learning project** that implements a Kafka-inspired message broker using only Java standard libraries. It supports:

- ✅ **Topics** – Named message streams
- ✅ **Producers** – Publish messages
- ✅ **Pull Consumers** – Fetch messages by offset
- ✅ **Push Subscribers** – Real-time message streaming
- ✅ **Persistence** – Disk-backed append-only logs
- ✅ **Recovery** – Crash-safe with CRC validation

## Quick Start

```bash
# Build
mvn clean compile

# Run broker
mvn exec:java -Dexec.mainClass="minibroker.Main"

# Broker listens on port 9092
```

## Tech Stack

- **Java 21+** (Virtual Threads)
- **Maven**
- No external dependencies

## Project Structure

```
src/main/java/minibroker/
├── log/        # Persistent storage
├── protocol/   # Binary wire protocol
├── server/     # TCP server
└── topic/      # Topic management
```

## Documentation

Comprehensive build guides in [`docs/`](docs/README.md).

## Why?

To deeply understand:
- Append-only log storage
- Binary protocol design
- Modern Java concurrency (Virtual Threads)
- Message delivery semantics

## License

MIT
