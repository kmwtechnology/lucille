---
title: Distributed
weight: 2
date: 2025-06-09
description: Running Lucille components as separate processes communicating via Kafka.
---

## Kafka-Distributed Mode

In distributed mode, each Lucille component runs as its own JVM process. All inter-component communication uses Kafka topics.

**Best for:** Large-scale batch ingests, multi-machine deployments, horizontal scaling.

### Starting the Runner

The Runner publishes documents to Kafka and waits for all work to complete:

```bash
java \
  -Dconfig.file=/path/to/config.conf \
  -cp 'lucille-core/target/lucille.jar:lucille-core/target/lib/*' \
  com.kmwllc.lucille.core.Runner \
  -usekafka
```

### Starting Workers

Start one or more Worker processes, each consuming from the Kafka source topic:

```bash
java \
  -Dconfig.file=/path/to/config.conf \
  -cp 'lucille-core/target/lucille.jar:lucille-core/target/lib/*' \
  com.kmwllc.lucille.core.Worker \
  my-pipeline-name
```

Adding more Worker processes increases throughput proportionally. Workers join the same Kafka consumer group — Kafka distributes partitions automatically.

### Starting the Indexer

```bash
java \
  -Dconfig.file=/path/to/config.conf \
  -cp 'lucille-core/target/lucille.jar:lucille-core/target/lib/*' \
  com.kmwllc.lucille.core.Indexer \
  my-pipeline-name
```

### Long-Running Workers and Indexers

Worker and Indexer processes are **long-running services** — they are not started or stopped by the Runner. A Runner invocation triggers a single batch run and exits when it completes, but the Workers and Indexers that processed its documents keep running and are ready to handle the next run immediately.

**Sequential runs share the same processes.** You can invoke the Runner repeatedly — nightly, hourly, or on demand — against the same pool of Workers and Indexers without restarting them. Each invocation is an independent run with its own accounting; the processes simply continue consuming from the source topic.

**Concurrent runs are supported.** Multiple Runner invocations can be active simultaneously, all handled by the same Workers and Indexers. Documents from different runs are interleaved on the Kafka source topic and processed without any per-run coordination. The constraint is that all concurrent runs must share the same pipeline name and indexer configuration.

**How run isolation works.** The Publisher stamps a unique `run_id` on every document it publishes. When a Worker or Indexer sends a lifecycle event (FINISH, FAIL, DROP) for a document, it reads the `run_id` from that document to determine which Kafka event topic to write to. Each run has its own dedicated event topic named `{pipeline}_event_{runId}`. The Publisher for each Runner invocation only consumes its own event topic, so completion accounting is completely isolated — concurrent runs do not interfere with each other.

### Kafka Configuration Block

All distributed-mode Kafka settings belong in a top-level `kafka {}` block shared by all components:

```hocon
kafka {
  bootstrapServers: "kafka1:9092,kafka2:9092"

  # Consumer group ID for all Lucille Workers
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

### Kafka Topic Naming

Lucille auto-creates Kafka topics named after the pipeline. For a pipeline named `my_pipeline`:

| Topic | Name | Purpose |
|---|---|---|
| Source | `my_pipeline_source` | Runner → Worker: documents to process |
| Destination | `my_pipeline_dest` | Worker → Indexer: processed documents |
| Event | `my_pipeline_event_{runId}` | Worker/Indexer → Publisher: lifecycle events |
| Fail | `my_pipeline_fail` | Dead-letter queue for poison-pill documents |

The per-run suffix on the event topic prevents events from one run interfering with another when multiple runs overlap. These topic names are useful to know when debugging with Kafka CLI tools.

**Important (batch mode only):** Because each batch run creates a new event topic (with a unique run_id in the name), the Lucille process must have Kafka Admin API permissions to create topics. Lucille creates the event topic explicitly via the Admin API to guarantee it has exactly 1 partition (required for event ordering). Kafka's `auto.create.topics.enable` is NOT sufficient because auto-created topics use the broker's default partition count, which would break the Publisher's accounting logic. If your Kafka cluster requires separate admin credentials, provide them via `kafka.adminPropertyFile`. This requirement does not apply in streaming mode — when events are disabled or when `kafka.eventTopic` is set to a fixed name, no new topics are created per run. See [Per-Run Event Topics]({{< relref "docs/architecture/internals/kafka-integration" >}}) for the architectural rationale.

The fail topic is written to when a document exceeds `worker.maxRetries` — the Worker routes the document there instead of crashing, and the rest of the ingest continues. Documents in the fail topic can be inspected with standard Kafka tooling, corrected if needed, and replayed by pointing a `FileConnector` (or a custom producer) at the topic. Requires `worker.maxRetries` and ZooKeeper to be configured; without them, a poison-pill document will crash and restart the Worker process repeatedly.

### Start Order

Start components in this order:
1. ZooKeeper (if using `worker.maxRetries`)
2. Kafka
3. Workers (start consuming before any documents arrive)
4. Indexer (initialize the search backend collection/index first, then start the Indexer process)
5. Runner (triggers the run once Workers and Indexer are ready)

The Indexer must be ready before the Runner starts. If the search backend collection does not yet exist, create it in the Indexer's startup script before launching the Indexer process — for example:

```bash
# Create Solr collection, then start Indexer
curl 'http://solr:8983/solr/admin/collections?action=CREATE&name=my-collection&numShards=1&collection.configName=_default'
java -Dconfig.file=/conf/config.conf -cp '/target/lib/*' com.kmwllc.lucille.core.Indexer my-pipeline
```
