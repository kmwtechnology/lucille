---
title: Publisher Accounting
weight: 4
date: 2025-06-09
description: >
  The Bag data structure, out-of-order event handling, the waitForCompletion loop, backpressure, and thread safety.
---

## Overview

The `Publisher` is Lucille's bookkeeper. It tracks every document from the moment it enters the system until it reaches a terminal state (indexed, failed, or dropped). This accounting is what allows the Runner to know when a connector's work is truly complete.

The core implementation lives in `PublisherImpl`, which maintains an in-memory ledger of pending documents. By design, the Publisher does **not** remember all documents it has ever published — only those currently in-flight. This keeps memory bounded regardless of how many documents flow through the system.

## The Central Data Structure: `docIdsToTrack`

```java
private final Bag<String> docIdsToTrack = SynchronizedBag.synchronizedBag(new HashBag<>());
```

This is the Publisher's primary ledger — a synchronized `Bag<String>` from Apache Commons Collections. Every document ID that is currently "in-flight" (published but not yet terminal) lives here.

### Why a Bag Instead of a Set

A `Bag` (multiset) allows duplicate entries. This matters because the same document ID can legitimately appear multiple times in a single run. If a connector publishes two documents with ID "doc-1", the Publisher expects to receive **two** separate terminal events for that ID. With a `Set`, removing the ID after the first terminal event would leave the second document untracked. With a `Bag`, each `remove` call decrements the count by one:

```java
// Two documents with same ID published → bag count is 2
docIdsToTrack.add("doc-1");  // count: 1
docIdsToTrack.add("doc-1");  // count: 2

// First terminal event → count drops to 1
docIdsToTrack.remove("doc-1", 1);  // count: 1

// Second terminal event → count drops to 0
docIdsToTrack.remove("doc-1", 1);  // count: 0, now removed
```

The `SynchronizedBag` wrapper ensures thread safety since `publish()` and `handleEvent()` run on different threads.

## The Secondary Ledger: `docIdsIndexedBeforeTracking`

```java
private final Bag<String> docIdsIndexedBeforeTracking = SynchronizedBag.synchronizedBag(new HashBag<>());
```

This handles a race condition with child documents. When a Worker creates a child document during pipeline processing, two things happen asynchronously:

1. A `CREATE` event is sent to the Publisher (so it starts tracking the child)
2. The child is processed and eventually reaches a terminal state (`FINISH` or `FAIL`)

These events can arrive **out of order**. If the terminal event arrives before the `CREATE` event, the Publisher can't find the ID in `docIdsToTrack`. Rather than ignoring this, it records the ID in `docIdsIndexedBeforeTracking`. When the late `CREATE` event eventually arrives, the Publisher checks this secondary ledger first:

```java
// In handleEvent(), when event.isCreate():
if (!docIdsIndexedBeforeTracking.remove(docId, 1)) {
    docIdsToTrack.add(docId);
}
```

If the ID is found in `docIdsIndexedBeforeTracking`, the Publisher knows the child already completed — no need to start tracking it.

## The `waitForCompletion` Polling Loop

This is the method that blocks the main thread until all work is done:

```java
public PublisherResult waitForCompletion(ConnectorThread thread, int timeout) throws Exception {
    while (true) {
        Event event = messenger.pollEvent();
        if (event != null) {
            handleEvent(event);
        }
        // Three termination conditions:
        if (!thread.isAlive() && !hasPending() && event == null) {
            return new PublisherResult(!thread.hasException(), null);
        }
    }
}
```

The loop terminates when **all three conditions** are met simultaneously:

1. **Connector thread is dead** (`!thread.isAlive()`) — no more documents will be published
2. **No pending documents** (`!hasPending()`) — every published document and child has reached a terminal state
3. **Event queue is empty** (`event == null`) — the previous poll returned nothing, meaning no more events are in transit

Condition 3 is critical. Even if conditions 1 and 2 are met, there might be events still in the queue that would change the pending count (e.g., a `CREATE` event for a child that hasn't been accounted for yet).

The `messenger.pollEvent()` call is a blocking operation with a timeout (typically 50ms for local, 2000ms for Kafka), preventing a busy-wait while still checking termination conditions periodically.

## Thread Interaction: `handleEvent()` vs `publish()`

The Publisher is designed for concurrent access from two threads:

- **Connector thread** calls `publish()` — adds IDs to `docIdsToTrack`
- **Main thread** (in `waitForCompletion`) calls `handleEvent()` — removes IDs from `docIdsToTrack`

Both methods mutate `docIdsToTrack`, which is why it must be a `SynchronizedBag`. The `publish()` method can also be called from multiple connector threads simultaneously (except in collapsing mode).

## The `maxPendingDocs` Backpressure Mechanism

When configured, this prevents the connector from overwhelming downstream components:

```java
private final ReentrantLock lockForPendingDocs = new ReentrantLock();
private final Condition pendingDocsBelowMaxCondition = lockForPendingDocs.newCondition();
```

In `publish()`, if the pending count exceeds the threshold, the calling thread blocks:

```java
if (maxPendingDocs != null) {
    lockForPendingDocs.lock();
    while (docIdsToTrack.size() >= maxPendingDocs) {
        pendingDocsBelowMaxCondition.await(10, TimeUnit.SECONDS);
    }
    lockForPendingDocs.unlock();
}
```

In `handleEvent()`, when a terminal event reduces the pending count below the max, blocked threads are signaled:

```java
if (docIdsToTrack.size() < maxPendingDocs) {
    pendingDocsBelowMaxCondition.signalAll();
}
```

The 10-second timeout on `await()` is a safety net — if a signal is somehow missed, the thread will re-check the condition periodically.

**Important concurrency note**: If N threads are blocked on `publish()` and the pending count drops to `maxPendingDocs - 1`, all N threads are signaled simultaneously. Each may then publish a document, causing the actual pending count to temporarily exceed `maxPendingDocs` by up to N-1. This is acceptable because each thread will block again on its next `publish()` call.

## Collapsing Mode

When `isCollapsing == true`, consecutive documents with the same ID are merged into one:

```java
private void publishInternal(Document document) throws Exception {
    if (!isCollapsing) {
        sendForProcessing(document);
        return;
    }
    if (previousDoc == null) {
        previousDoc = document;
        return;
    }
    if (previousDoc.getId().equals(document.getId())) {
        previousDoc.setOrAddAll(document);  // merge fields
    } else {
        sendForProcessing(previousDoc);
        previousDoc = document;
    }
}
```

The Publisher holds onto the previous document. If the next document has the same ID, fields are merged. If the ID differs, the previous document is finally sent for processing. The `flush()` method handles the last held document.

**Thread safety caveat**: Collapsing mode is NOT thread-safe for multiple publishing threads because `previousDoc` is shared mutable state without synchronization.

## `numPublished` vs `numReceived`

- `numReceived` — incremented every time `publish()` completes (counts inputs)
- `numPublished` — incremented every time `sendForProcessing()` is called (counts outputs)

In non-collapsing mode, these are equal. In collapsing mode, `numPublished <= numReceived` because multiple inputs may collapse into one output.

## Registration Ordering

A critical invariant: the document ID is added to `docIdsToTrack` **before** the document is placed on the processing queue:

```java
private void sendForProcessing(Document document) throws Exception {
    document.initializeRunId(runId);
    String docId = document.getId();

    // Track FIRST
    docIdsToTrack.add(docId);

    try {
        // Send SECOND
        messenger.sendForProcessing(document);
    } catch (Exception e) {
        // Rollback tracking if send fails
        docIdsToTrack.remove(docId, 1);
        throw e;
    }
    numPublished.incrementAndGet();
}
```

If the order were reversed (send first, then track), a fast Worker could process the document and emit a terminal event before the Publisher starts tracking it. The event would then be misclassified as "early" and placed in `docIdsIndexedBeforeTracking`, corrupting the accounting.

## Pause/Resume Mechanism

The Publisher supports pausing all publishing threads:

```java
private final ReentrantLock lockForPauseResume = new ReentrantLock();
private volatile Condition resumeCondition = null;
```

`pause()` creates a `Condition` object. Any thread calling `publish()` checks for this condition and blocks if it's set:

```java
if (resumeCondition != null) {
    lockForPauseResume.lock();
    if (resumeCondition != null) {  // double-check after acquiring lock
        while (resumeCondition != null) {
            resumeCondition.await();  // loop handles spurious wakeups
        }
    }
    lockForPauseResume.unlock();
}
```

`resume()` signals all waiting threads and nulls out the condition. The double-checked locking pattern (check volatile field, then acquire lock and re-check) avoids lock contention in the common case where the Publisher is not paused.

## Thread Safety Summary

| Field | Protection | Accessed By |
|-------|-----------|-------------|
| `docIdsToTrack` | `SynchronizedBag` | publish thread(s) + event handling thread |
| `docIdsIndexedBeforeTracking` | `SynchronizedBag` | event handling thread only (in practice) |
| `numReceived` | `AtomicLong` | multiple publish threads |
| `numPublished` | `AtomicLong` | multiple publish threads |
| `numCreated/Failed/Succeeded/Dropped` | unsynchronized `long` | event handling thread only |
| `previousDoc` | none (collapsing mode is single-thread only) | single publish thread |
| `maxPendingDocs` blocking | `ReentrantLock` + `Condition` | publish thread(s) + event thread |
| `pause/resume` | `ReentrantLock` + volatile `Condition` | publish thread(s) + external caller |
| `firstDocStopWatch` | `volatile` + `synchronized` block | publish thread(s) |
| `timerContext` | `ThreadLocal` | per-thread |
