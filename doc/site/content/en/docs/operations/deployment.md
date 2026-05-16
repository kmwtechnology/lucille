---
title: Production Deployment
weight: 2
date: 2025-06-09
description: Best practices and patterns for running Lucille in production, including Kubernetes deployments.
---

For a conceptual overview of each mode with architecture diagrams, see [Topology]({{< relref "docs/architecture/overview/topology" >}}).

## Deployment Modes

Choose a deployment mode based on your scale requirements. The same pipeline configuration runs in all modes — switching modes requires only a command-line flag change, not a code change.

| Mode | When to Use | Command |
|---|---|---|
| **Local** | Development, small jobs (< millions of docs) | `java -cp ... com.kmwllc.lucille.core.Runner` |
| **Kafka-Local** | Testing Kafka semantics without separate processes | `java -cp ... com.kmwllc.lucille.core.Runner -usekafka -local` |
| **Kafka-Distributed** | Production scale-out with multiple workers | Separate Runner, Worker, and Indexer processes |
| **Test** | Integration tests | Used via Lucille's test infrastructure only |

## Local Mode (Single JVM)

All components (Connector, Workers, Indexer) run as threads in a single JVM. Communication uses in-memory queues.

**Best for:** Development, scheduled batch jobs, single-machine production runs.

```bash
java \
  -Xmx4g \
  -Dconfig.file=/path/to/config.conf \
  -cp 'lucille-core/target/lucille.jar:lucille-core/target/lib/*' \
  com.kmwllc.lucille.core.Runner
```

To scale up throughput in local mode, increase the number of Worker threads:

```hocon
worker {
  threads: 8
}
```

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

## Docker Compose Deployment

Docker Compose is the simplest way to run Lucille in distributed mode. The [lucille-distributed-example](https://github.com/kmwtechnology/lucille/tree/main/lucille-examples/lucille-distributed-example) provides a complete working template. The key structure:

```yaml
services:
  zookeeper:
    # Bundled with Kafka; required for Kafka's internal coordination
    image: ...

  kafka:
    depends_on: [zookeeper]
    healthcheck:
      test: ["CMD", "kafka-topics.sh", "--bootstrap-server=kafka:9092", "--list"]

  worker:
    depends_on:
      kafka: { condition: service_healthy }
    entrypoint: java -Dconfig.file=/conf/config.conf -cp '/target/lib/*'
                     com.kmwllc.lucille.core.Worker my-pipeline

  indexer:
    depends_on:
      kafka: { condition: service_healthy }
      solr:  { condition: service_healthy }
    healthcheck:
      # Delegate healthcheck to the search backend — Runner waits for this before starting
      test: curl -f 'http://solr:8983/solr/...'
    entrypoint: |
      # Create the collection, then start the Indexer
      curl 'http://solr:8983/solr/admin/collections?action=CREATE&name=quickstart...'
      java -Dconfig.file=/conf/config.conf -cp '/target/lib/*'
           com.kmwllc.lucille.core.Indexer my-pipeline

  runner:
    depends_on:
      kafka:   { condition: service_healthy }
      indexer: { condition: service_healthy }  # Ensures Indexer + backend are ready
    entrypoint: java -Dconfig.file=/conf/config.conf -cp '/target/lib/*'
                     com.kmwllc.lucille.core.Runner -usekafka
```

Key design points from the example:
- **All components share one config file** — `config.file` points to the same `.conf` for all containers.
- **Classpath is `target/lib/*`** — Maven's `dependency:copy-dependencies` puts all JARs there; the main lucille JAR and all dependencies are sibling files.
- **The Indexer container's health check proxies the search backend's health** — this causes Docker Compose to hold the Runner until the search backend is ready, without the Runner needing to know anything about it.
- **Workers start before the Runner** — Kafka consumer group rebalancing takes time; workers should be consuming before the Runner begins publishing documents.

## Streaming Mode (Connectorless)

In streaming mode, Lucille runs without a Runner or Connector. An external system places documents onto a Kafka source topic directly, and one or more Worker or WorkerIndexer processes consume and process them continuously — with no run boundary or completion accounting.

**When to use:** Keeping a search index in sync with a live system (e.g., consuming from a CDC topic, Debezium, or any Kafka producer). The pipeline logic is identical to batch mode; only the execution model changes.

### Starting Workers in Streaming Mode

```bash
java \
  -Dconfig.file=/path/to/config.conf \
  -cp 'lucille-core/target/lucille.jar:lucille-core/target/lib/*' \
  com.kmwllc.lucille.core.Worker \
  my-pipeline-name
```

Or use `WorkerIndexer` to co-locate processing and indexing in one process:

```bash
java \
  -Dconfig.file=/path/to/config.conf \
  -cp 'lucille-core/target/lucille.jar:lucille-core/target/lib/*' \
  com.kmwllc.lucille.core.WorkerIndexer \
  my-pipeline-name
```

External documents must be placed onto the topic named `{pipeline-name}_source` (or the topic named by `kafka.sourceTopic` if set).

### Consuming from Multiple Topics with a Pattern

When running a `WorkerIndexer` in streaming mode, `kafka.sourceTopic` is interpreted as a Java regex pattern rather than a literal topic name. This means a single `WorkerIndexer` process can consume from multiple Kafka topics simultaneously by setting `sourceTopic` to a pattern that matches all of them:

```hocon
kafka {
  bootstrapServers: "kafka:9092"
  sourceTopic: "orders_.*_source"   # matches orders_us_source, orders_eu_source, etc.
  events: false
}
```

Kafka's consumer group protocol assigns partitions from all matching topics to the consumer. As new matching topics are created, the consumer group rebalances and picks them up automatically.

Note: this behavior applies to `WorkerIndexer` only. A standalone `Worker` subscribes to a single exact topic name and does not support pattern matching.

### Configuration for Streaming Mode

Streaming mode uses the same pipeline and Kafka configuration as distributed batch mode, with two differences:

- **No `connectors` block** — external producers push documents to the Kafka source topic.
- **Set `kafka.events: false`** to disable event publishing (since there is no Publisher to receive them), or set a fixed `kafka.eventTopic` if you still need event tracking:

```hocon
kafka {
  bootstrapServers: "kafka:9092"
  events: false   # No Runner/Publisher to receive events
}

pipelines: [
  {
    name: "my-pipeline"
    stages: [ ... ]
  }
]

indexer { type: "OpenSearch" }
opensearch { ... }
```

### Batch vs. Streaming: Same Pipeline Logic

The same Stage implementations work in both modes. Develop and test enrichment logic in batch mode (where `RunType.TEST` and the run summary make correctness verification straightforward), then deploy the same pipeline configuration in streaming mode for real-time production ingestion. For real-world search systems — which typically need an initial batch backfill followed by continuous updates — this means one pipeline definition, not two.

## Kubernetes Deployment

Lucille's architecture maps naturally onto Kubernetes primitives.

### Batch Jobs as Kubernetes CronJobs

For scheduled batch ingests, package Lucille as a container and run it as a `CronJob`. When the run completes, the container exits — there is no long-running process to manage between runs.

**Dockerfile:**
```dockerfile
FROM eclipse-temurin:17-jre

WORKDIR /app
COPY lucille-core/target/lucille.jar lucille.jar
COPY lucille-core/target/lib/ lib/
COPY config.conf config.conf

CMD ["java", \
     "-Xmx4g", \
     "-Dconfig.file=/app/config.conf", \
     "-cp", "/app/lucille.jar:/app/lib/*", \
     "com.kmwllc.lucille.core.Runner"]
```

**CronJob:**
```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: lucille-nightly-ingest
spec:
  schedule: "0 2 * * *"  # 2am daily
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: OnFailure
          containers:
          - name: lucille
            image: my-registry/lucille:latest
            env:
            - name: OPENSEARCH_URL
              valueFrom:
                secretKeyRef:
                  name: opensearch-credentials
                  key: url
            resources:
              requests:
                memory: "2Gi"
                cpu: "1"
              limits:
                memory: "4Gi"
                cpu: "4"
```

### Distributed Deployment as Kubernetes Pods

In distributed mode, each Lucille component runs as its own pod:

**Worker Deployment:**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: lucille-workers
spec:
  replicas: 4  # Scale by changing replicas
  selector:
    matchLabels:
      app: lucille-worker
  template:
    metadata:
      labels:
        app: lucille-worker
    spec:
      containers:
      - name: worker
        image: my-registry/lucille:latest
        command: ["java", "-cp", "/app/lucille.jar:/app/lib/*",
                  "com.kmwllc.lucille.core.Worker", "my-pipeline"]
        env:
        - name: config.file
          value: /app/config.conf
        resources:
          requests:
            memory: "2Gi"
            cpu: "2"
```

Workers are the natural scaling target. When the Kafka source topic backlog grows, increase `replicas`. Kubernetes' Horizontal Pod Autoscaler can drive this automatically using Kafka consumer group lag as a metric (via KEDA or similar).

## Memory Sizing

Rule of thumb: `worker.threads × (Stage resource usage)`. A pipeline that loads a 500MB NLP model uses 500MB × number of Worker threads.

| Component | Typical Heap Size |
|---|---|
| Runner (local, lightweight pipeline) | 512MB–2GB |
| Runner (local, ML stages) | 4GB–16GB per stage model × threads |
| Worker (distributed, no ML) | 512MB–1GB |
| Worker (distributed, with ML models) | 2GB–8GB per model |
| Indexer | 512MB–1GB |

Always set `-Xmx` explicitly. Avoid letting Java use all available RAM.

## Backpressure and Queue Sizing

In local mode, set `publisher.queueCapacity` to bound the number of in-flight documents:

```hocon
publisher {
  queueCapacity: 10000
}
```

In distributed mode, use `publisher.maxPendingDocs` to throttle the Connector:

```hocon
publisher {
  maxPendingDocs: 80000
}
```

Without backpressure, a fast Connector can publish faster than Workers process, causing out-of-memory conditions.

## Indexer Batch Tuning

The Indexer sends documents in batches. Both batch size and timeout are independently configurable:

```hocon
indexer {
  batchSize: 200        # Flush when this many docs accumulate
  batchTimeout: 5000    # Flush after 5 seconds if batchSize is not reached
}
```

Larger batches improve indexing throughput (fewer API calls). The timeout prevents documents from waiting indefinitely when volume is low.

## Graceful Shutdown

Lucille handles `SIGINT` (Ctrl+C) and `SIGTERM` gracefully. On receiving a signal:
1. The Connector stops publishing new documents.
2. Workers drain the remaining documents.
3. The Indexer flushes its current batch.
4. The process exits with a run summary.

Do not send `SIGKILL` — documents in flight may not be indexed.

## Monitoring

See [Logging]({{< relref "docs/operations/logging" >}}) for setting up log monitoring.

Key metrics logged periodically during a run:

```
INFO PublisherImpl: 37029 docs published. One minute rate: 3225.69 docs/sec. Waiting on 21014 docs.
INFO WorkerPool: 27017 docs processed. One minute rate: 1787.10 docs/sec. Mean pipeline latency: 10.63 ms/doc.
INFO Indexer: 17016 docs indexed. One minute rate: 455.07 docs/sec. Mean backend latency: 6.90 ms/doc.
```

The frequency is controlled by `log.seconds` (default: 30).

## Run Summary

At the end of every run, Lucille prints a structured summary:

```
RUN SUMMARY: Success. 1/1 connectors complete. All published docs succeeded.
connector1: complete. 200000 docs succeeded. 0 docs failed. 0 docs dropped. Time: 416.47 secs.
Run took 417.46 secs.
```

A connector that failed entirely is distinguished from one that completed with individual document failures. Subsequent connectors after a failure are listed as skipped.

## Production Checklist

- [ ] Set `-Xmx` heap limit appropriate for your pipeline's memory usage.
- [ ] Configure `publisher.queueCapacity` (local mode) or `publisher.maxPendingDocs` (distributed).
- [ ] Tune `indexer.batchSize` and `indexer.batchTimeout` for your backend's throughput.
- [ ] Use environment variable substitution for credentials (`${?VAR_NAME}`) — never hard-code secrets in config files.
- [ ] Route `DocLogger` output to a separate file in production (see [Logging]({{< relref "docs/operations/logging" >}})).
- [ ] Enable `runner.metricsLoggingLevel: "INFO"` for stage-by-stage metrics at run completion.
- [ ] Configure `worker.maxRetries` and ZooKeeper if poison-pill protection is needed — without this, a document that repeatedly crashes a Worker will stall the pipeline indefinitely. Failed documents are routed to the `{pipeline}_fail` topic for inspection and replay.
- [ ] Set `runner.connectorTimeout` if any connector might run longer than 24 hours (the default timeout).
