---
title: "Distributed Streaming"
weight: 3
date: 2025-06-09
description: Running Lucille without a Runner for continuous ingestion from Kafka.
---

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
- **No Publisher** — there is no run boundary and no completion accounting.

#### Run IDs in Streaming Mode

In batch mode, the Publisher stamps a `run_id` on every document at publication time. In streaming mode there is no Publisher, so documents arriving from an external system typically have no `run_id` field. This is normal and expected — the `run_id` is a batch-mode concept.

Consequences of absent run_ids:
- The Worker's MDC will have no `run_id` context in log lines (the field is null).
- If events are enabled, the default event topic name would be `{pipeline}_event_null` (since the topic name is derived from the run_id).

If your external system *wants* to stamp a `run_id` on documents (e.g., to correlate a batch of updates), it can include a `run_id` field in the JSON before placing it on the Kafka topic. The Worker will read it and use it for MDC logging. But this is entirely optional.

#### Event Configuration for Streaming Mode

| Scenario | Setting | Why |
|---|---|---|
| No event tracking needed | `kafka.events: false` | Most common for streaming. No Publisher is listening, so events would go unread. |
| Events needed for external monitoring | `kafka.eventTopic: "lucille_events"` | Sends all events to a fixed topic name. An external system can consume this topic to track document success/failure. |
| Events needed with per-document run_ids | `kafka.events: true` (default) | Only works if the external producer stamps `run_id` on documents. Events route to `{pipeline}_event_{runId}`. |

**Recommendation:** Set `kafka.events: false` unless you have a specific consumer for the event topic. If you do need events, set `kafka.eventTopic` to a fixed name to avoid the `_event_null` topic naming issue.

```hocon
kafka {
  bootstrapServers: "kafka:9092"

  # Option A: Disable events entirely (most common for streaming)
  events: false

  # Option B: Send events to a fixed topic for external monitoring
  # events: true
  # eventTopic: "lucille_events"
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
