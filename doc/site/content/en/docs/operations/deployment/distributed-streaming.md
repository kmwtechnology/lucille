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
  -Dlog4j2.configurationFile=classpath:lucille-log4j2.xml \
  -Dconfig.file=/path/to/config.conf \
  -cp 'lucille-all/target/lib/*' \
  com.kmwllc.lucille.core.Worker \
  my-pipeline-name
```

Or use `WorkerIndexer` to co-locate processing and indexing in one process:

```bash
java \
  -Dlog4j2.configurationFile=classpath:lucille-log4j2.xml \
  -Dconfig.file=/path/to/config.conf \
  -cp 'lucille-all/target/lib/*' \
  com.kmwllc.lucille.core.WorkerIndexer \
  my-pipeline-name
```

External documents must be placed onto the topic named `{pipeline-name}_source` (or the topic named by `kafka.sourceTopic` if set).

### Consuming from Multiple Topics with a Pattern

When running a `WorkerIndexer` in streaming mode, `kafka.sourceTopic` can be a Java regex pattern instead of a topic name. Lucille decides which it is from the characters in the value:

- A value made only of letters, digits, `.`, `_` and `-` is a literal topic name, and the `WorkerIndexer` subscribes to that one topic by name. In such a name `.` is a literal dot: `orders.v1` matches only the topic `orders.v1`, not `ordersXv1`.
- Any other value is a Java regex, and the `WorkerIndexer` subscribes to every topic it matches.

At startup the `WorkerIndexer` logs which form it used ("by name" or "by pattern").

A single `WorkerIndexer` process can consume from multiple Kafka topics simultaneously by setting `sourceTopic` to a pattern that matches all of them:

```hocon
kafka {
  bootstrapServers: "kafka:9092"
  sourceTopic: "orders_.*_source"   # matches orders_us_source, orders_eu_source, etc.
  events: false
}
```

Kafka's consumer group protocol assigns partitions from all matching topics to the consumer. As new matching topics are created, the consumer group rebalances and picks them up automatically.

Prefer a literal name when one topic is enough. A consumer subscribed by pattern requests metadata for every topic in the cluster, and keeps refreshing it, so that it can match new topics. On a cluster with many topics and one consumer per worker thread, these requests put a heavy load on the brokers.

As with a standalone `Worker`, a consumer subscribed by name can cause a broker running with `auto.create.topics.enable=true` to create the source topic if it does not exist. Set `kafka.consumer."allow.auto.create.topics": false` to prevent that.

Note: pattern matching applies to `WorkerIndexer` only. A standalone `Worker` always subscribes to a single exact topic name.

### Configuration for Streaming Mode

Streaming mode uses the same pipeline and Kafka configuration as distributed batch mode, with two differences:

- **No `connectors` block** — external producers push documents to the Kafka source topic.
- **No Publisher** — there is no run boundary and no completion accounting.

Streaming deployments usually set `worker.maxRetries`, which enables the fail topic. When it is enabled, the fail topic (`{pipeline}_fail`, or `kafka.failTopic`) must exist before a `Worker` or `WorkerIndexer` starts, unless the broker auto-creates topics: each checks for it at startup and does not start without it. See [Kafka Topic Setup]({{< relref "docs/operations/deployment/distributed-batch#kafka-topic-setup" >}}).

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
| Events must say more about the document than its id (e.g. whether a `FINISH` was a delete) | `kafka.event.includeFields: ["deleted", ...]` | Copies the listed document fields onto each event as `documentFields`. Only small, flat values are copied; see [Enriching Events with Document Fields]({{< relref "docs/architecture/internals/kafka-integration#enriching-events-with-document-fields" >}}). |
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

  # Optionally copy selected document fields onto each event (requires events: true)
  # event {
  #   includeFields: ["source", "deleted"]
  # }
}

pipelines: [
  {
    name: "my-pipeline"
    stages: [ ... ]
  }
]

indexer {
  opensearch { ... }
}
```

### Batch vs. Streaming: Same Pipeline Logic

The same Stage implementations work in both modes. Develop and test enrichment logic in batch mode (where `RunType.TEST` and the run summary make correctness verification straightforward), then deploy the same pipeline configuration in streaming mode for real-time production ingestion. For real-world search systems — which typically need an initial batch backfill followed by continuous updates — this means one pipeline definition, not two.
