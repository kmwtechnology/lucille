---
title: "Distributed Batch"
weight: 2
date: 2025-06-09
description: Running Lucille components as separate processes communicating via Kafka.
---

## Kafka-Distributed Mode

In distributed mode, each Lucille component runs as its own JVM process. All inter-component communication uses Kafka topics.

**Best for:** Large-scale batch ingests, multi-machine deployments, horizontal scaling.

---

## Prerequisites

Before starting any Lucille component in distributed mode, ensure:

- **Kafka** is running and reachable from all machines that will run Lucille components
- **Kafka topics** are created for each pipeline referenced by your connectors (or auto-creation is enabled on the broker) — see [Kafka Topic Setup](#kafka-topic-setup) for details
- **Workers and Indexer** are running for each pipeline referenced by your connectors — see [Starting Each Component](#starting-each-component)
- **Search backend** collection/index exists (Solr, OpenSearch, Elasticsearch, etc.)
- **ZooKeeper** is running (only if using `worker.maxRetries` for poison-pill protection)
- **Same config file** is available to all Lucille processes (Runner, Workers, Indexer)

---

## Start Order

Start components in this order:

1. **ZooKeeper** (if using `worker.maxRetries`)
2. **Kafka**
3. **Workers** — start consuming before any documents arrive
4. **Indexer** — create the search backend collection/index first, then start the Indexer process
5. **Runner** — triggers the run once Workers and Indexer are ready

The Runner should be started **last**. It immediately begins publishing documents to Kafka. If Workers or the Indexer aren't ready, documents will queue on the source topic (which is fine) but the run won't complete until they start consuming.

---

## Kafka Configuration

All distributed-mode Kafka settings belong in a top-level `kafka {}` block shared by all components:

```hocon
kafka {
  bootstrapServers: "kafka1:9092,kafka2:9092"

  # Consumer group ID for all Lucille Workers and Indexers
  consumerGroupId: "lucille_workers"

  # Maximum time between polls before consumer is evicted from consumer group
  maxPollIntervalSecs: 600

  # Maximum request size in bytes — increase for large documents
  maxRequestSize: 250000000

  # TLS/SASL — provide Java properties files for detailed Kafka client config
  securityProtocol: "SSL"
  consumerPropertyFile: "/path/to/consumer.properties"
  producerPropertyFile: "/path/to/producer.properties"
  adminPropertyFile: "/path/to/admin.properties"

  # Custom document serializer/deserializer (override only if needed)
  documentDeserializer: "com.kmwllc.lucille.message.KafkaDocumentDeserializer"
  documentSerializer: "com.kmwllc.lucille.message.KafkaDocumentSerializer"

  # Set to false to disable event tracking (FINISH/FAIL/DROP events won't be published)
  events: true

  # Override source topic name (default: auto-generated from pipeline name)
  sourceTopic: "my-pipeline-source"

  # CAUTION: Setting eventTopic disables per-run topic isolation. Only use this
  # in streaming/connectorless mode where no Runner is coordinating the run.
  # In batch mode with a Runner, omit this — Lucille generates a per-run event topic
  # automatically to prevent cross-run event interference.
  # eventTopic: "lucille_events"
}
```

---

## Kafka Topic Setup

Lucille uses Kafka topics to pass documents between components. If your Kafka broker has `auto.create.topics.enable` set to `true` (the default), you don't need to create any topics yourself. If auto-create is disabled, you must pre-create certain topics before starting Lucille.

### If auto-create is enabled

Lucille will create topics automatically on first use. No manual setup is required. However:

- Auto-created topics use the broker's `num.partitions` default (typically 1). This limits parallelism — see the partition guidance below.
- Lucille does not clean up topics after a run. Event topics accumulate over time (one per run). Consider a retention policy or periodic cleanup.
- The event topic is an exception: Lucille always creates it explicitly via the Kafka Admin API with exactly 1 partition, regardless of `auto.create.topics.enable`. This requires Admin API permissions on the Kafka cluster.

### If auto-create is disabled

Create the following topics before starting Lucille:

| Topic | Default name | Override | Partitions | Created by |
|---|---|---|---|---|
| Source | `{pipeline}_source` | `kafka.sourceTopic` | ≥ total Worker threads across all processes | Admin |
| Dest | `{pipeline}_dest` | — (not overridable) | ≥ number of Indexer processes (typically 1) | Admin |
| Event | `{pipeline}_event_{runId}` | `kafka.eventTopic` | Exactly 1 (required for ordering) | Lucille creates this automatically via Admin API |
| Fail | `{pipeline}_fail` | — (not overridable) | ≥ 1 | Admin (only needed if `worker.maxRetries` is configured) |

**Example** — for a pipeline named `my-pipeline` with 8 Worker threads:

```bash
kafka-topics.sh --create --topic my-pipeline_source \
  --partitions 8 --replication-factor 2 \
  --bootstrap-server kafka:9092

kafka-topics.sh --create --topic my-pipeline_dest \
  --partitions 1 --replication-factor 2 \
  --bootstrap-server kafka:9092

# Only if worker.maxRetries is configured:
kafka-topics.sh --create --topic my-pipeline_fail \
  --partitions 1 --replication-factor 2 \
  --bootstrap-server kafka:9092
```

You do **not** need to create the event topic — Lucille creates it automatically at the start of each run via the Admin API. This requires that the Kafka user Lucille connects with has `CreateTopics` permission. If your Kafka cluster requires separate admin credentials, provide them via `kafka.adminPropertyFile`.

### Topic details

**Source topic** (`{pipeline}_source`)

The Runner publishes documents here; Workers consume from it. The partition count determines the maximum number of Worker threads that can consume in parallel — excess threads beyond the partition count will sit idle. Set the partition count to at least the total number of Worker threads you plan to run across all Worker processes.

Override the name with `kafka.sourceTopic` in your config if you need a custom topic name (e.g., when consuming from a topic managed by another system in streaming mode).

**Dest topic** (`{pipeline}_dest`)

Workers publish processed documents here; the Indexer consumes from it. A single partition is sufficient for most deployments because the Indexer is rarely the bottleneck — it sends batched bulk requests to the search backend and is I/O-bound on the backend's write capacity, not on Kafka consumption. Multiple partitions are only needed if you run multiple Indexer processes (uncommon).

The name is not overridable — it is always `{pipeline}_dest`.

**Event topic** (`{pipeline}_event_{runId}`)

Workers and Indexers publish lifecycle events here (CREATE, FINISH, FAIL, DROP); the Publisher on the Runner process consumes them to track run completion. This topic **must** have exactly 1 partition to guarantee FIFO ordering — if events arrive out of order, the Publisher's accounting logic can be corrupted (e.g., a child's FINISH arriving before its CREATE).

Lucille creates this topic explicitly via the Kafka Admin API at the start of each batch run. In batch mode, each run gets its own event topic (the run ID is in the topic name), which prevents events from one run interfering with another when multiple runs overlap. You do not pre-create it.

In streaming mode (no Runner), you can either disable events entirely (`kafka.events: false`) or set a fixed topic name (`kafka.eventTopic: "my_fixed_event_topic"`) and pre-create it with 1 partition.

**Fail topic** (`{pipeline}_fail`)

The Worker publishes poison-pill documents here when they exceed `worker.maxRetries`. This topic is only used when `worker.maxRetries` is configured (which also requires ZooKeeper). If you don't use retry tracking, this topic is never written to and doesn't need to exist.

Documents in the fail topic can be inspected with standard Kafka tooling, corrected if needed, and replayed by pointing a connector at the topic. The topic relies on auto-creation or must be pre-created by an admin — Lucille does not create it via the Admin API.

The name is not overridable — it is always `{pipeline}_fail`.

### Permissions required

The Kafka user Lucille connects with needs:

| Permission | Reason |
|---|---|
| `CreateTopics` | Lucille creates the event topic via Admin API at the start of each batch run |
| `Produce` on source topic | The Runner publishes documents to the source topic |
| `Produce` on dest topic | Workers publish processed documents to the dest topic |
| `Produce` on event topic | Workers and Indexers publish lifecycle events |
| `Produce` on fail topic | Workers publish poison-pill documents (if `maxRetries` is configured) |
| `Consume` on source topic | Workers consume documents for processing |
| `Consume` on dest topic | Indexer consumes documents for indexing |
| `Consume` on event topic | The Runner's Publisher consumes lifecycle events |

---

## Starting Each Component

### Runner

The Runner publishes documents to Kafka and waits for all work to complete:

```bash
java \
  -Dconfig.file=/path/to/config.conf \
  -cp 'lucille-core/target/lucille.jar:lucille-core/target/lib/*' \
  com.kmwllc.lucille.core.Runner \
  -usekafka
```

### Workers

Start one or more Worker processes, each consuming from the Kafka source topic:

```bash
java \
  -Dconfig.file=/path/to/config.conf \
  -cp 'lucille-core/target/lucille.jar:lucille-core/target/lib/*' \
  com.kmwllc.lucille.core.Worker \
  my-pipeline-name
```

Adding more Worker processes increases throughput proportionally. Workers join the same Kafka consumer group — Kafka distributes partitions automatically.

### Indexer

```bash
java \
  -Dconfig.file=/path/to/config.conf \
  -cp 'lucille-core/target/lucille.jar:lucille-core/target/lib/*' \
  com.kmwllc.lucille.core.Indexer \
  my-pipeline-name
```

If the search backend collection does not yet exist, create it before launching the Indexer:

```bash
# Create Solr collection, then start Indexer
curl 'http://solr:8983/solr/admin/collections?action=CREATE&name=my-collection&numShards=1&collection.configName=_default'
java -Dconfig.file=/conf/config.conf -cp '/target/lib/*' com.kmwllc.lucille.core.Indexer my-pipeline
```

---

## Long-Running Workers and Indexers

Worker and Indexer processes are **long-running services** — they are not started or stopped by the Runner. A Runner invocation triggers a single batch run and exits when it completes, but the Workers and Indexers that processed its documents keep running and are ready to handle the next run immediately.

**Sequential runs share the same processes.** You can invoke the Runner repeatedly — nightly, hourly, or on demand — against the same pool of Workers and Indexers without restarting them. Each invocation is an independent run with its own accounting; the processes simply continue consuming from the source topic.

**Concurrent runs are supported.** Multiple Runner invocations can be active simultaneously, all handled by the same Workers and Indexers. Documents from different runs are interleaved on the Kafka source topic and processed without coordination or interaction between runs.

**Multiple pipelines can coexist.** A single Lucille config can define multiple connectors feeding different pipelines. In distributed mode, each pipeline has its own set of Kafka topics (`{pipeline}_source`, `{pipeline}_dest`). You can run separate fleets of Workers and Indexers for each pipeline — start Workers with `com.kmwllc.lucille.core.Worker pipeline-a` and separately `com.kmwllc.lucille.core.Worker pipeline-b`. The Runner publishes each connector's documents to the correct pipeline's source topic based on the connector's `pipeline` config property.

The constraint is: for each pipeline referenced by a connector in your config, Workers and an Indexer must be running for that pipeline before the Runner starts. If a connector references `pipeline: "enrichment"`, there must be Workers consuming from `enrichment_source` and an Indexer consuming from `enrichment_dest`.

**Multi-connector runs** execute connectors sequentially within a single Runner invocation. Each connector's documents are published to its pipeline's source topic, and the Runner waits for all of that connector's documents to reach a terminal state before starting the next connector. If a connector fails, subsequent connectors are skipped.

**How run isolation works.** The Publisher stamps a unique `run_id` on every document it publishes. When a Worker or Indexer sends a lifecycle event (FINISH, FAIL, DROP) for a document, it reads the `run_id` from that document to determine which Kafka event topic to write to. Each run has its own dedicated event topic named `{pipeline}_event_{runId}`. The Publisher for each Runner invocation only consumes its own event topic, so completion accounting is completely isolated — concurrent runs do not interfere with each other.
