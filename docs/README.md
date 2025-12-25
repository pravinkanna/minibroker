# MiniBroker Documentation Index

Complete documentation for building the MiniBroker message broker.

---

## Quick Start

1. Read [Concepts & Terminology](concepts_and_terminology.md) first
2. Review the [Technical Specification](message_broker_spec.md)
3. Follow the [Implementation Plan](implementation_plan.md)
4. Build phase by phase with detailed guides

---

## Documentation Map

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         MINIBROKER DOCS                                 │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────────┐    ┌──────────────────────────────────────┐    │
│  │ START HERE          │    │ REFERENCE                            │    │
│  ├─────────────────────┤    ├──────────────────────────────────────┤    │
│  │ • Concepts          │    │ • Architecture Decisions (ADR)       │    │
│  │ • Tech Spec         │    │ • Java NIO Deep Dive                 │    │
│  │ • Implementation    │    │ • Java Concurrency Patterns          │    │
│  │   Plan              │    │ • Troubleshooting Guide              │    │
│  └─────────────────────┘    └──────────────────────────────────────┘    │
│                                                                         │
│  ┌────────────────────────────────────────────────────────────────┐     │
│  │ PHASE-BY-PHASE BUILD GUIDES                                    │     │
│  ├────────────────────────────────────────────────────────────────┤     │
│  │ Phase 1: Log Engine      → Core storage implementation         │     │
│  │ Phase 2: Wire Protocol   → Binary protocol design              │     │
│  │ Phase 3: Broker Server   → TCP server with virtual threads     │     │
│  │ Phase 4: Push Subs       → Real-time subscriptions             │     │
│  │ Phase 5: Recovery        → Crash recovery & hardening          │     │
│  └────────────────────────────────────────────────────────────────┘     │
│                                                                         │
│  ┌────────────────────────────────────────────────────────────────┐     │
│  │ ADDITIONAL GUIDES                                              │     │
│  ├────────────────────────────────────────────────────────────────┤     │
│  │ • Testing Guide          → Unit, integration, chaos tests      │     │
│  │ • Client SDK Guide       → Producer, consumer clients          │     │
│  └────────────────────────────────────────────────────────────────┘     │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Document Descriptions

### Core Documents

| Document | Description | Read When |
| :--- | :--- | :--- |
| [concepts_and_terminology.md](concepts_and_terminology.md) | Message broker fundamentals, terminology glossary | First, before anything else |
| [message_broker_spec.md](message_broker_spec.md) | High-level technical specification | Understanding overall design |
| [implementation_plan.md](implementation_plan.md) | Day-by-day roadmap with milestones | Planning your work |

### Phase Guides

| Document | Description | Contains |
| :--- | :--- | :--- |
| [phase1_log_engine_guide.md](phase1_log_engine_guide.md) | Build the persistent log | `LogEntry`, `LogSegment`, `TopicLog` |
| [phase2_wire_protocol_guide.md](phase2_wire_protocol_guide.md) | Design binary protocol | `Command`, `ProtocolReader`, `ProtocolWriter` |
| [phase3_broker_server_guide.md](phase3_broker_server_guide.md) | Build TCP server | `BrokerServer`, `ClientHandler`, `Topic` |
| [phase4_push_subscriptions_guide.md](phase4_push_subscriptions_guide.md) | Implement push | Subscriber management, broadcasting |
| [phase5_recovery_hardening_guide.md](phase5_recovery_hardening_guide.md) | Add durability | Recovery, flush strategies, shutdown |

### Reference Documents

| Document | Description | Read When |
| :--- | :--- | :--- |
| [java_nio_deep_dive.md](java_nio_deep_dive.md) | ByteBuffer, FileChannel mastery | Working with file I/O |
| [java_concurrency_patterns.md](java_concurrency_patterns.md) | Virtual threads, locks, collections | Implementing thread safety |
| [architecture_decisions.md](architecture_decisions.md) | Why we made certain choices | Understanding rationale |

### Practical Guides

| Document | Description | Read When |
| :--- | :--- | :--- |
| [testing_guide.md](testing_guide.md) | Unit, integration, chaos testing | Writing tests |
| [client_sdk_guide.md](client_sdk_guide.md) | Producer, consumer implementations | Building clients |
| [troubleshooting_guide.md](troubleshooting_guide.md) | Common issues and fixes | Debugging problems |

---

## Recommended Reading Order

### For Building (Follow the Phases)

```
1. concepts_and_terminology.md    (30 min)
2. message_broker_spec.md         (30 min)
3. implementation_plan.md         (20 min)
4. phase1_log_engine_guide.md     (2-3 days of coding)
5. phase2_wire_protocol_guide.md  (1-2 days of coding)
6. phase3_broker_server_guide.md  (2-3 days of coding)
7. phase4_push_subscriptions_guide.md (1-2 days of coding)
8. phase5_recovery_hardening_guide.md (2-3 days of coding)
```

### For Reference (As Needed)

```
• java_nio_deep_dive.md           → When confused about ByteBuffer/FileChannel
• java_concurrency_patterns.md    → When implementing thread safety
• testing_guide.md                → When writing tests
• troubleshooting_guide.md        → When debugging issues
• architecture_decisions.md       → When wondering "why this design?"
```

---

## Project Structure

After completing all phases, your project will look like:

```
MiniBroker/
├── docs/                              ← You are here
│   ├── README.md                      ← This file
│   ├── concepts_and_terminology.md
│   ├── message_broker_spec.md
│   ├── implementation_plan.md
│   ├── phase1_log_engine_guide.md
│   ├── phase2_wire_protocol_guide.md
│   ├── phase3_broker_server_guide.md
│   ├── phase4_push_subscriptions_guide.md
│   ├── phase5_recovery_hardening_guide.md
│   ├── java_nio_deep_dive.md
│   ├── java_concurrency_patterns.md
│   ├── architecture_decisions.md
│   ├── testing_guide.md
│   ├── client_sdk_guide.md
│   └── troubleshooting_guide.md
├── pom.xml
└── src/
    ├── main/java/minibroker/
    │   ├── Main.java
    │   ├── config/
    │   │   └── BrokerConfig.java
    │   ├── log/
    │   │   ├── LogEntry.java
    │   │   ├── LogSegment.java
    │   │   └── TopicLog.java
    │   ├── protocol/
    │   │   ├── Command.java
    │   │   ├── ProtocolReader.java
    │   │   ├── ProtocolWriter.java
    │   │   ├── Request.java
    │   │   ├── PublishRequest.java
    │   │   ├── FetchRequest.java
    │   │   ├── SubscribeRequest.java
    │   │   └── ProtocolException.java
    │   ├── server/
    │   │   ├── BrokerServer.java
    │   │   └── ClientHandler.java
    │   ├── topic/
    │   │   ├── Topic.java
    │   │   └── TopicRegistry.java
    │   └── client/
    │       ├── MiniBrokerProducer.java
    │       ├── MiniBrokerConsumer.java
    │       └── MiniBrokerSubscriber.java
    └── test/java/minibroker/
        ├── log/
        │   ├── LogEntryTest.java
        │   ├── LogSegmentTest.java
        │   └── TopicLogTest.java
        ├── protocol/
        │   └── ProtocolRoundTripTest.java
        └── server/
            └── BrokerIntegrationTest.java
```

---

## What You'll Learn

By completing this project, you will deeply understand:

| Topic | Where Covered |
| :--- | :--- |
| Append-only log storage | Phase 1 |
| Binary protocol design | Phase 2 |
| Virtual Threads (Project Loom) | Phase 3, Concurrency Patterns |
| Java NIO (FileChannel, ByteBuffer) | Phase 1, NIO Deep Dive |
| Producer/Consumer patterns | Phase 4, Concurrency Patterns |
| Crash recovery | Phase 5 |
| At-least-once delivery | Concepts, Phase 1 |
| Push vs Pull consumption | Phase 3, Phase 4 |

---

Happy building! 🚀
