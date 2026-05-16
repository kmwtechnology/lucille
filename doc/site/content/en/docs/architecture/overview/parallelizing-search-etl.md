---
title: "Parallelizing Search ETL"
weight: 3
date: 2025-06-09
description: >
  How Lucille structures search ingestion as three concurrent components communicating through queues, and how it tracks document lifecycle across an asynchronous system.
---

To design any concurrent system, two questions must be answered:

1. What are the core components?
2. How do they communicate?

### The Core Components

Lucille derives its components directly from the structure of the problem. The [Problem of Search ETL]({{< relref "docs/architecture/overview/problem" >}}) established that search ingestion involves three distinct functions: acquiring data from a source, enriching it, and sending it to the search backend. Lucille maps each function to a dedicated component that runs independently:

**Connectors** connect to source systems to acquire data. A Connector reads from its source — a filesystem, a database, a Kafka topic, an RSS feed — and emits Documents into the system one at a time. It does not know how many Workers will process those documents, or how long enrichment will take.

**Workers** enrich documents by passing them through a Pipeline of Stages. Each Stage performs a specific transformation: extracting text, running NLP, generating embeddings, looking up database records. A Worker handles one document at a time and writes the result onward. Multiple Workers run concurrently, each with its own Pipeline instance, so enrichment scales with available CPU.

**Indexers** receive processed documents and send them in batches to the search backend. An Indexer accumulates documents until a batch is full or a timeout expires, then issues a single bulk API call. Batching is essential for search engine performance — bulk writes are significantly faster than one-at-a-time indexing.

These three components run concurrently and asynchronously. Each operates at its own pace, decoupled from the others. The system achieves optimal throughput given available resources because no component waits for another unless it exhausts its backlog of work — or the downstream queue is full, which provides natural backpressure on the upstream component.

### How Should They Communicate?

In some distributed systems, asynchronous components require tight, fine-grained communication. Consider a distributed query engine where a central coordinator assigns specific data partitions to worker nodes, monitors their progress with heartbeats, detects straggler tasks, re-assigns failed partitions to other nodes, and waits for explicit acknowledgment from every worker before declaring a query complete. The coordinator and workers exchange a continuous stream of control messages — assignment, acknowledgment, status updates, failure notifications — and the coordinator must maintain a detailed model of the state of every worker at all times.

Lucille takes a different approach. The key observation is that Connectors, Workers, and Indexers can be largely decoupled. No component needs to know what the others are doing in real time. A Connector doesn't need to know how many Workers exist or whether they are fast or slow — it just needs somewhere to put its output. A Worker doesn't need to know which Connector produced a document or which Indexer will consume it. An Indexer doesn't need to know where documents came from or how many are still in the pipeline behind them.

This means it suffices for the components to communicate via **queues**. Each component places its output on a queue for the next component to read from:

- The Connector puts documents on a **processing queue**.
- Workers read the processing queue, process documents through the Pipeline, and put results on an **indexing queue**.
- Indexers read the indexing queue and send documents to the search backend.

No component calls another directly. No component waits for a response. Each simply produces to a queue and consumes from a queue. The queue is the entire communication contract.

```java
// Shared queues connecting the three components.
Queue processingQueue;   // documents waiting to be enriched
Queue indexingQueue;     // enriched documents waiting to be indexed


// === CONNECTOR ===
while (source.hasNext()) {
    Document doc = Document.from(source.next());
    processingQueue.put(doc);
}


// === WORKER (N instances, running concurrently) ===
while (running) {
    Document doc = processingQueue.poll();
    Document result = pipeline.process(doc);
    indexingQueue.put(result);
}


// === INDEXER (M instances, running concurrently) ===
while (running) {
    Document doc = indexingQueue.poll();
    batch.add(doc);
    if (batch.isFull() || batch.isExpired()) {
        searchClient.bulkIndex(batch.flush());
    }
}
```

This is the core of Lucille's architecture. The Connector, Workers, and Indexers need no knowledge of each other beyond the existence of these two queues.

### What's Missing?

The pseudocode above captures the essential structure but leaves out something important: there is no way to know when a batch ingest is complete, no way to track how many documents have succeeded or failed, and no support for two common pipeline operations — dropping documents that should not be indexed, and emitting child documents from a single source record.

With the sequential loop, completion was trivial:

```java
while (source.hasNext()) { }
// loop returns → run is done
```

In a concurrent system, the Connector finishing is not enough. When the Connector stops putting documents on the processing queue, Workers may still be enriching documents, and Indexers may still be sending batches. The system is not done until every document has reached a terminal state — indexed, failed, or dropped.

Tracking this requires knowing about every document in flight: which ones have been introduced, which have completed, and which are still being processed. A Worker can also generate new child documents during pipeline processing — for example, splitting a source document into chunks for a RAG pipeline — and those children must be tracked independently.

One approach would be a central coordinator that determines completion by polling all components — querying the Connector, all Worker instances, and all Indexer instances, and declaring the run done when all queues are empty and all components report no work in flight. Lucille avoids this design: it would require every component to expose a status interface, and the coordinator would need to know the full topology of the running system. Instead, Lucille uses an event-driven model in which components report their own progress without being interrogated.

### The Solution: an Event Queue and a Publisher

Lucille's solution is to introduce two additional elements:

**An event queue** carries document lifecycle notifications from Workers and Indexers. When a document is successfully indexed, the Indexer sends a `FINISH` event. When a document fails, a `FAIL` event is sent. When a Stage deliberately drops a document, a `DROP` event is sent. When a Worker generates a child document, a `CREATE` event is sent so that child can be tracked.

**A Publisher** is used by the Connector to introduce documents into the system. The Publisher stamps each document with a run ID, records its ID as pending, and places it on the processing queue. It then listens to the event queue, reconciling events against its pending set. When a `FINISH`, `FAIL`, or `DROP` event arrives for a document ID, the Publisher removes it from the pending set. When a `CREATE` event arrives for a child document, the Publisher adds that new ID to the pending set.

The run is complete when three conditions hold simultaneously: the Connector has stopped, the event queue is empty, and the Publisher has no pending IDs. Any one condition alone is insufficient.

This design handles two subtle edge cases. First, a child document can be indexed and receive a `FINISH` event before the Publisher has processed its `CREATE` event, because Workers and Indexers run concurrently. The Publisher handles this with a secondary ledger of premature completions that it reconciles when the `CREATE` event eventually arrives. Second, the pending set is a `Bag` rather than a `Set` — two documents with the same ID can be published in the same run, and a `Bag` counts duplicates, so two publishes of the same ID require two separate terminal events to clear.

The complete pseudocode, now including the event queue and Publisher, is shown below. See [Appendix: Full Pseudocode](#appendix-full-pseudocode) for the detailed version presented as an appendix.

---

### Appendix: Full Pseudocode

```java
// === QUEUES ===

Queue processingQueue;  // documents waiting to be enriched by Workers
Queue indexingQueue;    // processed documents waiting to be sent to the search backend
Queue eventQueue;       // document lifecycle events (CREATE, FINISH, FAIL, DROP)


// === RUNNER / CONNECTOR ===
// Runner launches the connector in its own thread; the main thread waits for completion.

publisher = new Publisher(processingQueue, eventQueue);

new Thread(() -> {
    // publisher.publish() is thread-safe, so a connector can spawn multiple
    // publishing threads; this example calls publish() sequentially
    while (source.hasNext()) {
        Document doc = Document.from(source.next());
        publisher.publish(doc);  // stamps run ID, tracks doc ID, puts on processingQueue
    }
}).start();

// main thread: block until all documents (and their children) reach a terminal state
publisher.waitForCompletion();


// === WORKER (N instances) ===
// Each instance has its own Pipeline with its own instances of every Stage.
// Stages can hold stateful resources (models, connections, compiled patterns)
// without synchronization because they are never shared across threads.

pipeline = new Pipeline(stages);  // per-thread instance

while (running) {
    Document doc = processingQueue.poll();    // blocking poll with timeout
    Iterator<Document> results = pipeline.process(doc);

    for (Document result : results) {
        if (result.isChild()) {
            eventQueue.send(CREATE, result);  // notify publisher of new child
        }
        if (result.isDropped()) {
            eventQueue.send(DROP, result);    // notify publisher, discard document
        } else {
            indexingQueue.put(result);        // send for indexing
        }
    }
}


// === INDEXER (M instances) ===
// Each instance runs independently, consuming from the shared indexingQueue.

while (running) {
    Document doc = indexingQueue.poll();      // blocking poll with timeout

    batch.add(doc);

    if (batch.isFull() || batch.isExpired()) {
        try {
            searchClient.bulkIndex(batch.flush());
            for (Document indexed : batch) {
                eventQueue.send(FINISH, indexed);
            }
        } catch (Exception e) {
            for (Document failed : batch) {
                eventQueue.send(FAIL, failed);
            }
        }
    }
}


// === PUBLISHER (event loop, runs on main thread) ===
// Reconciles lifecycle events to determine when all work is complete.

Bag<String> pending = {};  // IDs not yet in a terminal state

on publish(doc):
    pending.add(doc.id);

on event(CREATE, id):
    pending.add(id);

on event(FINISH | FAIL | DROP, id):
    pending.remove(id);

isComplete():
    return connector.isDone() && eventQueue.isEmpty() && pending.isEmpty();
```

Nothing in this pseudocode specifies what the queues actually are — whether they are in-memory data structures or distributed messaging infrastructure. That question is addressed in the next page: [Pluggable Queueing and the Deployment Model]({{< relref "docs/architecture/overview/pluggable-queueing" >}}).
