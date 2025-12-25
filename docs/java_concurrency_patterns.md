# MiniBroker: Java Concurrency Patterns

Deep dive into concurrency patterns used in MiniBroker.

---

## Table of Contents

1. [Virtual Threads Explained](#1-virtual-threads-explained)
2. [Lock Strategies](#2-lock-strategies)
3. [Thread-Safe Collections](#3-thread-safe-collections)
4. [Atomic Operations](#4-atomic-operations)
5. [Producer-Consumer Pattern](#5-producer-consumer-pattern)
6. [Common Pitfalls](#6-common-pitfalls)

---

## 1. Virtual Threads Explained

### What Are Virtual Threads?

Before Java 21:
- **Platform Thread** = 1:1 mapping to OS thread
- Each thread ~1MB stack memory
- Max ~10,000 threads practical limit

After Java 21:
- **Virtual Thread** = lightweight thread managed by JVM
- Each thread ~few KB
- Millions of threads possible

### How They Work

```
┌─────────────────────────────────────────────────────────────────┐
│                          JVM                                    │
│  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐        │
│  │ Virt 1 │ │ Virt 2 │ │ Virt 3 │ │ Virt 4 │ │ Virt N │ ...    │
│  └───┬────┘ └───┬────┘ └───┬────┘ └───┬────┘ └───┬────┘        │
│      │          │          │          │          │              │
│      └──────────┼──────────┼──────────┼──────────┘              │
│                 │          │          │                         │
│           ┌─────▼──────────▼──────────▼─────┐                   │
│           │     Carrier Thread Pool          │                   │
│           │   (= number of CPU cores)        │                   │
│           └─────────────────────────────────┘                   │
└─────────────────────────────────────────────────────────────────┘
                          │
                          ▼
            ┌─────────────────────────────┐
            │       OS Kernel             │
            │   Platform Threads          │
            └─────────────────────────────┘
```

When a virtual thread blocks (I/O, sleep), the JVM:
1. **Unmounts** it from the carrier thread
2. Runs another virtual thread on that carrier
3. **Remounts** the blocked thread when it's ready

### Usage in MiniBroker

```java
// Create executor that spawns virtual threads
ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

// Each connection gets a virtual thread
while (running) {
    Socket client = serverSocket.accept();
    executor.submit(() -> {
        // This code runs in a virtual thread
        // Blocking here is CHEAP
        handleClient(client);
    });
}
```

### Blocking is Now Cheap

```java
// OLD mindset: "Blocking is bad, use async"
CompletableFuture.supplyAsync(() -> readFromSocket())
    .thenApply(this::parse)
    .thenAccept(this::handle);

// NEW mindset: "Just block, it's fine"
byte[] data = socket.getInputStream().read(); // Virtual thread unmounts
Response response = process(data);
socket.getOutputStream().write(response);     // Virtual thread unmounts
```

### When Virtual Threads DON'T Unmount

**Pinning** occurs when a virtual thread is stuck on a carrier:

1. **Inside synchronized blocks**
   ```java
   synchronized (lock) {
       channel.write(buffer); // Virtual thread is PINNED
   }
   // FIX: Use ReentrantLock instead
   lock.lock();
   try {
       channel.write(buffer); // Can unmount
   } finally {
       lock.unlock();
   }
   ```

2. **Native/JNI calls** - can't do much about this

---

## 2. Lock Strategies

### Strategy 1: ReentrantLock (Recommended)

```java
private final ReentrantLock lock = new ReentrantLock();

public void write(byte[] data) throws IOException {
    lock.lock();
    try {
        // Exclusive access
        channel.write(ByteBuffer.wrap(data));
    } finally {
        lock.unlock(); // Always in finally!
    }
}
```

**Properties**:
- Virtual thread-friendly (can unmount while waiting)
- Supports fairness (`new ReentrantLock(true)`)
- Supports tryLock with timeout

### Strategy 2: ReadWriteLock

When reads >> writes:

```java
private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

public void write(byte[] data) {
    rwLock.writeLock().lock();
    try {
        // Exclusive - no other readers or writers
        doWrite(data);
    } finally {
        rwLock.writeLock().unlock();
    }
}

public byte[] read(long position) {
    rwLock.readLock().lock();
    try {
        // Shared - other readers OK, but no writers
        return doRead(position);
    } finally {
        rwLock.readLock().unlock();
    }
}
```

**Note**: FileChannel.read(buf, position) is already thread-safe for reads at different positions, so ReadWriteLock may be overkill for MiniBroker.

### Strategy 3: Lock-Free with Atomics

For simple counters/flags:

```java
private final AtomicLong nextOffset = new AtomicLong(0);

public long getAndIncrementOffset() {
    return nextOffset.getAndIncrement();
}
```

### Lock Best Practices

```java
// 1. Always release in finally
lock.lock();
try {
    doWork();
} finally {
    lock.unlock();
}

// 2. Keep critical sections small
lock.lock();
try {
    // ONLY the part that needs synchronization
    index.add(position);
    nextOffset++;
} finally {
    lock.unlock();
}
// Do expensive work OUTSIDE the lock
channel.force(true);

// 3. Use tryLock for timeouts
if (lock.tryLock(5, TimeUnit.SECONDS)) {
    try {
        doWork();
    } finally {
        lock.unlock();
    }
} else {
    throw new TimeoutException("Could not acquire lock");
}
```

---

## 3. Thread-Safe Collections

### ConcurrentHashMap

For the topic registry:

```java
private final ConcurrentHashMap<String, Topic> topics = new ConcurrentHashMap<>();

// Thread-safe get-or-create
public Topic getOrCreate(String name) {
    return topics.computeIfAbsent(name, this::createTopic);
}
```

**Key methods**:
```java
map.computeIfAbsent(key, k -> new Value());  // Atomic get-or-create
map.compute(key, (k, v) -> transform(v));    // Atomic update
map.forEach((k, v) -> process(k, v));        // Concurrent iteration
```

### CopyOnWriteArrayList

For subscribers (few writes, many reads):

```java
private final List<Subscriber> subscribers = new CopyOnWriteArrayList<>();

// Safe to iterate while modifying
public void broadcast(Message msg) {
    for (Subscriber sub : subscribers) {  // Snapshot iteration
        try {
            sub.send(msg);
        } catch (IOException e) {
            subscribers.remove(sub);  // Concurrent modification OK!
        }
    }
}
```

**When to use**:
- Iteration >> modification
- Can tolerate slightly stale reads
- Small collections

**When NOT to use**:
- Frequent adds/removes (copying is expensive)
- Large collections

### BlockingQueue

For producer-consumer patterns:

```java
private final BlockingQueue<Message> queue = new LinkedBlockingQueue<>(1000);

// Producer
public void produce(Message msg) throws InterruptedException {
    queue.put(msg);  // Blocks if queue full
}

// Consumer
public void consume() throws InterruptedException {
    while (true) {
        Message msg = queue.take();  // Blocks if queue empty
        process(msg);
    }
}
```

---

## 4. Atomic Operations

### AtomicLong

```java
private final AtomicLong counter = new AtomicLong(0);

long next = counter.getAndIncrement();      // Returns old value, then increments
long current = counter.incrementAndGet();   // Increments, then returns new value
counter.set(100);                           // Simple set
long value = counter.get();                 // Simple get
counter.compareAndSet(expected, newValue);  // CAS operation
```

### AtomicReference

For reference swapping:

```java
private final AtomicReference<LogSegment> activeSegment = new AtomicReference<>();

public void rollover() {
    LogSegment newSegment = createNewSegment();
    LogSegment oldSegment = activeSegment.getAndSet(newSegment);
    oldSegment.close();
}
```

### LongAdder

For high-contention counters (better than AtomicLong):

```java
private final LongAdder messageCount = new LongAdder();

public void onMessage() {
    messageCount.increment();  // Low contention
}

public long getCount() {
    return messageCount.sum();  // May be slightly stale
}
```

---

## 5. Producer-Consumer Pattern

### Pattern: Async Message Broadcast

Instead of blocking publisher threads to broadcast:

```java
public class Topic {
    private final BlockingQueue<LogEntry> broadcastQueue = new LinkedBlockingQueue<>();
    private final Thread broadcaster;
    
    public Topic() {
        broadcaster = Thread.ofVirtual().start(this::broadcastLoop);
    }
    
    public long publish(byte[] data) throws IOException {
        long offset = log.append(data);
        LogEntry entry = log.read(offset);
        broadcastQueue.offer(entry);  // Non-blocking
        return offset;
    }
    
    private void broadcastLoop() {
        while (!Thread.interrupted()) {
            try {
                LogEntry entry = broadcastQueue.take();  // Blocks
                for (Subscriber sub : subscribers) {
                    sub.send(entry);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
```

**Benefits**:
- Publish returns immediately
- Broadcast failures don't affect publisher
- Can batch broadcasts

### Pattern: Periodic Flusher

```java
public class LogSegment {
    private final ScheduledExecutorService scheduler = 
        Executors.newSingleThreadScheduledExecutor();
    
    public LogSegment(Path path) {
        // Flush every 500ms
        scheduler.scheduleAtFixedRate(
            this::backgroundFlush,
            500, 500, TimeUnit.MILLISECONDS
        );
    }
    
    private void backgroundFlush() {
        try {
            lock.lock();
            try {
                if (dirty) {
                    channel.force(false);
                    dirty = false;
                }
            } finally {
                lock.unlock();
            }
        } catch (IOException e) {
            System.err.println("Flush failed: " + e);
        }
    }
}
```

---

## 6. Common Pitfalls

### Pitfall 1: Using synchronized with Virtual Threads

```java
// BAD - pins virtual thread
public synchronized void write(byte[] data) {
    channel.write(ByteBuffer.wrap(data)); // PINNED!
}

// GOOD - virtual-thread-friendly
private final ReentrantLock lock = new ReentrantLock();

public void write(byte[] data) {
    lock.lock();
    try {
        channel.write(ByteBuffer.wrap(data)); // Can unmount
    } finally {
        lock.unlock();
    }
}
```

### Pitfall 2: Lock Ordering Deadlock

```java
// Thread 1: locks A, then B
// Thread 2: locks B, then A
// DEADLOCK!

// FIX: Always lock in same order
private static final Object LOCK_A = new Object();
private static final Object LOCK_B = new Object();

// Both threads must lock A before B
```

### Pitfall 3: Forgetting to Unlock

```java
// BAD - exception causes deadlock
lock.lock();
doWorkThatMayThrow(); // If this throws, lock is never released!
lock.unlock();

// GOOD - always unlock in finally
lock.lock();
try {
    doWorkThatMayThrow();
} finally {
    lock.unlock();
}
```

### Pitfall 4: Broken Double-Check Locking

```java
// BAD - broken without volatile
private Cache cache;

public Cache getCache() {
    if (cache == null) {
        synchronized (this) {
            if (cache == null) {
                cache = new Cache(); // May be seen partially constructed!
            }
        }
    }
    return cache;
}

// GOOD - use volatile
private volatile Cache cache;

// OR BETTER - use holder pattern
private static class Holder {
    static final Cache INSTANCE = new Cache();
}
public Cache getCache() {
    return Holder.INSTANCE;
}
```

### Pitfall 5: Not Handling InterruptedException

```java
// BAD - swallowing interrupt
try {
    Thread.sleep(1000);
} catch (InterruptedException e) {
    // Ignored - thread doesn't know it was interrupted
}

// GOOD - restore interrupt flag
try {
    Thread.sleep(1000);
} catch (InterruptedException e) {
    Thread.currentThread().interrupt(); // Restore flag
    return; // Exit the operation
}
```

---

## Quick Reference

```
┌─────────────────────────────────────────────────────────────────┐
│                    CONCURRENCY CHEAT SHEET                      │
├─────────────────────────────────────────────────────────────────┤
│ Virtual Threads:                                                │
│   Executors.newVirtualThreadPerTaskExecutor()                   │
│   Thread.ofVirtual().start(runnable)                            │
├─────────────────────────────────────────────────────────────────┤
│ Locks:                                                          │
│   ReentrantLock - basic mutex, virtual-thread-friendly          │
│   ReadWriteLock - multiple readers OR one writer                │
│   StampedLock   - optimistic reads (advanced)                   │
├─────────────────────────────────────────────────────────────────┤
│ Collections:                                                    │
│   ConcurrentHashMap - thread-safe map                           │
│   CopyOnWriteArrayList - snapshot iteration                     │
│   BlockingQueue - producer-consumer                             │
├─────────────────────────────────────────────────────────────────┤
│ Atomics:                                                        │
│   AtomicLong/Integer - thread-safe counters                     │
│   AtomicReference - thread-safe reference swap                  │
│   LongAdder - high-contention counters                          │
├─────────────────────────────────────────────────────────────────┤
│ Golden Rules:                                                   │
│   1. Use ReentrantLock, not synchronized (for virtual threads)  │
│   2. Always unlock in finally                                   │
│   3. Keep critical sections small                               │
│   4. Always restore interrupt flag                              │
│   5. Lock in consistent order to avoid deadlock                 │
└─────────────────────────────────────────────────────────────────┘
```
