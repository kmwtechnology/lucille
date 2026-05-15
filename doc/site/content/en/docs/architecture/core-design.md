---
title: "Core Architecture"
weight: 2
date: 2025-06-09
description: >
  How Lucille maps the three functions of search ETL to concurrent components communicating through pluggable queues.
---


Lucille maps each function of search ETL to a specific component:

- **Connectors** connect to source systems to acquire data.
- **Workers** enrich documents by passing them through a Pipeline of Stages.
- **Indexers** send documents in batches to the search backend.

These three components run concurrently and asynchronously. Each operates at its own pace, decoupled from the others. The system achieves optimal performance given available resources because no component waits for another unless it exhausts its backlog of work — or the downstream queue is full, providing natural backpressure.

### Message-Driven Communication via Queues

In designing a concurrent architecture like this, a key question is how the components should communicate. Lucille is a message-driven architecture where components interact via queues.

The components don't need to exchange detailed control or coordination messages with each other. Each component simply places its output on a queue so it can be consumed by the next component:

- The Connector puts documents on a **processing queue**.
- Workers read the processing queue, process documents through the Pipeline, and put results on an **indexing queue**.
- Indexers read the indexing queue and send documents to the search engine.

### Pluggable Queue Implementation

Given that each component's interaction with its queue reduces to `put()` and `poll()`, the queue implementation can be made pluggable. Lucille's components are designed so they don't have to "care" about what kind of queue they're using.

Lucille achieves this through a **Messenger** abstraction. Each component type has its own Messenger interface (`PublisherMessenger`, `WorkerMessenger`, `IndexerMessenger`) that defines the messaging operations it needs. The component code is identical regardless of which Messenger implementation is in use. This provides a significant advantage:

**For a simple deployment**, the Connector, Workers, and Indexer all run as threads inside a single JVM where the queues are in-memory `LinkedBlockingQueue` instances with fixed capacity (the `LocalMessenger` implementation).

**To scale the deployment**, the Connector, Workers, and Indexers can run in separate JVMs where the queues are Kafka topics (the `KafkaMessenger` implementations).

In transitioning from a single-JVM deployment to a distributed deployment, the only thing that changes is the Messenger implementation. All other code paths remain the same — the pipeline stages, the accounting logic, the retry behavior, the batching. This means that when prototyping and testing a single-JVM deployment, you can have a high level of confidence that the system will continue to behave the same way — but with better performance — as you move to a distributed model.

### Pseudocode: From Sequential Loop to Concurrent Architecture

Compare the sequential loop above to how Lucille restructures the same work into independent components communicating through queues:

```java
// === QUEUES ===
// In local mode: LinkedBlockingQueue instances in a single JVM.
// In distributed mode: Kafka topics across multiple JVMs.

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

### Testing Benefits of Pluggable Queuing

Moving from a sequential loop to a concurrent architecture introduces a new problem: the system becomes harder to test. A simple loop with mocked externals runs trivially inside a unit test. A multi-threaded, queue-based system does not — unless it has been specifically designed to facilitate this.

The pluggable queuing model solves this problem directly. A primary goal of Lucille is to make it easy to write end-to-end tests of ingestion pipelines, even though the system is concurrent.

Lucille has a dedicated **test mode** where a `TestMessenger` wraps the `LocalMessenger` and records a history of every message sent between components — every document published, every document sent for indexing, every lifecycle event. After a test run, test code can retrieve this history and make assertions about what happened, without having to mock the components of Lucille itself. The mocking can focus on the external services that would be connected to (the source system, the search backend), while the Lucille system works as it normally does, right inside the test.

The search backend is bypassed entirely in test mode, so tests run fast and don't require infrastructure. The practical effect is that integration tests are straightforward: run the pipeline in test mode and assert against the captured history.

### Determining Completion of a Batch Ingest

One major detail we've left out is how to determine the completion of a batch ingest. This problem was trivial in the simple loop:

```java
while (source.hasNext()) { }
```

When processing is sequential, we simply keep going until there are no more records to retrieve from the source system.

In a concurrent system like Lucille, the problem is more challenging. The Connector "knows" when the source system has no more records, but it is not exchanging fine-grained control messages with the other components. It doesn't even "know" how many Worker instances there are — all it does is pump documents onto a queue. The Workers themselves can generate new "child" documents during pipeline processing. Neither the Connector nor the Workers "know" how many Indexers there are.

One approach would be a central coordinator that determines completion by polling all components. It would need to know about the Connector, all Worker instances, all Indexer instances, and it could determine completion by detecting that all queues are empty and all components report no work in flight.

Lucille aims for simplicity and has avoided the idea of a central coordinator that knows about all components. Instead, the design relies on a **Publisher** component and an **event-driven accounting model**:

- A batch ingest is considered "done" when all documents reach the end of their lifecycle — either being indexed successfully, erroring out, or being deliberately dropped.

- The **Publisher** is used by the Connector to introduce Documents into the system. From a deployment standpoint, it is more of a utility used by the Connector than a standalone component with its own deployment. The Publisher is responsible for tracking the IDs of all published documents that have not yet completed. It can "forget" about documents once it learns they are complete — it does not need to remember the full history of all documents, only the currently-pending ones.

- Components report lifecycle events by placing them on an **event queue**. If a document errors out during pipeline processing or indexing, a FAIL event is sent. If a child document is created during pipeline processing, a CREATE event is sent. If a document is deliberately dropped, a DROP event is sent. When a document is indexed successfully, a FINISH event is sent.

- The Publisher listens to these events and reconciles them. When it receives a CREATE event for a child document, it begins tracking that ID. When it later receives a FINISH event for that document, it stops tracking it.

- The run is complete when three conditions are simultaneously true: (1) the Connector has stopped, (2) the event queue is empty, and (3) the Publisher has no pending IDs. All three must hold together — any one alone is insufficient.

This design handles subtle edge cases. Child documents can complete before the Publisher even receives their CREATE event (because the Worker sends CREATE and the Indexer sends FINISH asynchronously). The Publisher handles this with a secondary ledger of "early completions" that it reconciles when the CREATE event eventually arrives. The accounting uses a `Bag` rather than a `Set` because two documents with the same ID can be published in the same run — the Bag counts duplicates, so two publishes of the same ID require two separate terminal events to clear.

### Batch and Streaming Models

So far we have been talking about "batch ingests" where there is a fixed amount of data to be retrieved from the source system. A goal of Lucille is to support both batch and streaming models.

In a streaming scenario, there is an unbounded supply of documents arriving in the system and we need to process and index them as they arrive. There is no run boundary, no completion accounting, no connector lifecycle.

From an architectural perspective it is straightforward for Lucille to support a streaming model because Lucille is a queue-based system. To deploy Lucille in streaming mode, you omit the Connector and Publisher components. An external system takes the Connector's role by writing directly to the processing queue (a Kafka topic). Workers consume from that topic, and the rest of the system functions as normal — Workers write to an indexing queue, Indexers read from that queue and send documents to the search backend.

A Stage that extracts entities or generates embeddings does not know or care which mode it is running under. You can develop and test enrichment logic in batch mode — where the accounting and test infrastructure make correctness verification straightforward — and then deploy the same Pipeline in streaming mode for real-time production ingestion.

### The WorkerIndexer: A Practical Middle Ground

As a simplification of the fully distributed approach, Lucille provides an option to couple each Worker instance with an Indexer instance running in the same JVM. Instead of having an external queue between separate Worker and Indexer processes, you have a **WorkerIndexer** process where the Worker writes to an in-memory queue that the co-located Indexer reads from.

The WorkerIndexer still reads from Kafka (so it participates in the distributed consumer group for horizontal scaling) but avoids the overhead of a second Kafka round-trip between Worker and Indexer. This gives horizontal scaling — you can run multiple WorkerIndexer processes — without the operational complexity of managing separate Worker and Indexer fleets. It is a useful middle ground between the fully local single-JVM deployment and the fully distributed model with independent Worker and Indexer processes.

---
