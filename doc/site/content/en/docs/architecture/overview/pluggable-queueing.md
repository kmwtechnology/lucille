---
title: "Pluggable Queueing and the Deployment Model"
weight: 4
date: 2025-06-09
description: >
  How Lucille's Messenger abstraction makes the queue implementation pluggable, enabling a flexible deployment model and straightforward testing.
---

The [previous page]({{< relref "docs/architecture/overview/parallelizing-search-etl" >}}) showed how to parallelize search ingestion using three concurrent components — Connector, Workers, and Indexers — communicating through a processing queue and an indexing queue. What we have not yet discussed is what those queues actually are.

### What a Queue Must Provide

The queues in Lucille's architecture have a specific set of requirements. They must support concurrent access from multiple producers and consumers. They must support a blocking `poll()` operation so that a Worker or Indexer that finds an empty queue waits efficiently rather than spinning. And when multiple Workers are calling `poll()` concurrently, work must be distributed fairly among them — each document should go to exactly one Worker.

### The Case Against Hardcoding

In a project like Lucille, it would have been tempting to choose a suitable queue implementation and hardcode it directly into the components that use it. But either obvious choice would have carried a significant cost.

**Hardcoding `LinkedBlockingQueue`** would limit scaling to what can be achieved by adding threads inside a single JVM. For large-scale ingestion that needs to be distributed across multiple machines, this ceiling is too low.

**Hardcoding Kafka** would require every user to have a running Kafka cluster — even for a simple ingest that does not need to scale beyond one JVM. For development, testing, and smaller production workloads, this is unnecessary infrastructure with real operational overhead.

### The Key Observation: Queues Are Used in a Limited, Standardized Way

Lucille's components interact with queues in a narrow and well-defined way: producers call `put()`, consumers call `poll()`. There is no complex queue-specific logic embedded in the component code — no partition management, no consumer group coordination, no topic configuration. Since the interface is this simple, the queue implementation can be made **pluggable** without changing any component code.

Lucille achieves this through a **Messenger** abstraction. Each component type has its own Messenger interface — `PublisherMessenger`, `WorkerMessenger`, `IndexerMessenger` — that defines exactly the messaging operations that component needs. The Connector and Publisher use a `PublisherMessenger` to put documents on the processing queue and read from the event queue. Workers use a `WorkerMessenger` to poll the processing queue and put results on the indexing queue. Indexers use an `IndexerMessenger` to poll the indexing queue and send events.

Each component is written entirely against its Messenger interface. The component code is identical regardless of which Messenger implementation is in use — it has no knowledge of whether the underlying queue is an in-memory data structure or a Kafka topic.

### A Flexible Deployment Model and a Natural Scaling Path

The pluggable Messenger model directly enables a flexible deployment model.

**For a simple deployment**, the Connector, Workers, and Indexer all run as threads inside a single JVM where the queues are in-memory `LinkedBlockingQueue` instances with fixed capacity. This is the `LocalMessenger` implementation. No external infrastructure is required.

**To scale the deployment**, the Connector, Workers, and Indexers can run in separate JVMs where the queues are Kafka topics. This is the `KafkaMessenger` implementation. Kafka's consumer group protocol handles partition assignment and rebalancing as Workers are added or removed.

This gives Lucille a natural scaling path. You can begin with a single-JVM deployment, gain experience with the system, tune your pipeline, and validate your configuration — all without Kafka. When your data volumes require more throughput than a single JVM can provide, you introduce Kafka and distribute the load across multiple machines. The only thing that changes in this transition is the Messenger implementation. Every other code path — the pipeline stages, the accounting logic, the retry behavior, the batching — remains exactly the same. This gives a high degree of confidence that the system will behave identically at scale to how it behaved in a single-JVM deployment.

### Testability

The pluggable Messenger model also has significant advantages for testing.

Moving from a sequential loop to a concurrent, queue-based architecture introduces a new challenge: the system becomes harder to test. A simple loop with mocked externals runs trivially inside a unit test. A multi-threaded, queue-based system with concurrent components does not — unless it has been specifically designed to make testing straightforward.

The Messenger abstraction solves this directly. Lucille provides a **test mode** where a `TestMessenger` wraps the `LocalMessenger` and records a history of every message sent between components — every document published, every document sent for indexing, every lifecycle event. After a test run, test code can retrieve this history and make assertions about exactly what happened.

The Indexer runs in **bypass mode** during tests: the full indexing code path executes — batching, event sending, error handling — right up to the point of communicating with the search backend, which is bypassed. This means the Indexer's logic is exercised without requiring a live Solr or OpenSearch instance.

Connectors and pipeline stages that communicate with live source systems do need some form of mocking for those external systems — for example, using WireMock to simulate an HTTP endpoint, or an in-memory database for a JDBC connector. But that mocking is scoped to the external system itself. The Lucille components — the Workers, the Indexer, the Publisher, the event queue — are not mocked at all. They run exactly as they would in production.

The practical effect is that end-to-end integration tests of complex pipelines are straightforward to write. The full Lucille system runs inside a unit test, in its complete and realistic form, without requiring Lucille's own components to be stubbed out. The confidence this provides is significant: a pipeline that passes its tests in this mode has been proved to work with the real Lucille mechanics, not a simplified approximation of them.

---

The pages so far have focused on the batch ingest model — a bounded run with a defined start and end. Lucille also supports a streaming model for continuous, unbounded ingestion, as well as a hybrid `WorkerIndexer` deployment pattern that offers a practical middle ground between single-JVM and fully distributed. These are covered in the next page: [Topology]({{< relref "docs/architecture/overview/topology" >}}).
