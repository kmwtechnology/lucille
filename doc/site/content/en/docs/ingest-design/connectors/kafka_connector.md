---
title: Kafka Connector
weight: 4
date: 2025-06-09
description: A Connector that reads Documents from a Kafka topic and publishes them into the Lucille pipeline.
---

[Source Code](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/KafkaConnector.java)

The `KafkaConnector` reads Documents from a Kafka topic and publishes them into a Lucille pipeline. This is distinct from Kafka's role as the *messaging layer* in distributed mode. The `KafkaConnector` is a data *source*, reading documents produced by an upstream system.

## Use Cases

- **Streaming ingest from Kafka:** An upstream application publishes documents (as JSON) to a Kafka topic, and Lucille reads them for enrichment and indexing.
- **Connectorless distributed mode:** In this deployment pattern, a third-party publisher puts documents onto a Kafka source topic, and Lucille Workers consume them directly. In this case the `KafkaConnector` is not used. Workers listen to the source topic directly.

## Configuration

All Kafka connection parameters are nested under the `kafka` key within the connector config block.

```hocon
connectors: [
  {
    name: "kafka-source"
    class: "com.kmwllc.lucille.connector.KafkaConnector"
    pipeline: "my-pipeline"

    kafka.bootstrapServers: "kafka1:9092,kafka2:9092"
    kafka.topic: "my-source-topic"
    kafka.consumerGroupId: "lucille-kafka-connector"
    kafka.clientId: "lucille-consumer-1"
    kafka.maxPollIntervalSecs: 600
    idField: "article_id"
  }
]
```

## Configuration Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `kafka.bootstrapServers` | String | Yes | Comma-separated list of Kafka broker addresses. |
| `kafka.topic` | String | Yes | Kafka topic to consume from. |
| `kafka.consumerGroupId` | String | Yes | Consumer group ID. |
| `kafka.clientId` | String | Yes | Kafka client identifier. |
| `kafka.maxPollIntervalSecs` | Integer | Yes | Maximum time in seconds between polls before the consumer is removed from the consumer group. Sets the consumer's `max.poll.interval.ms`. The Spec declares it as a String, so an integer or a numeric string is accepted, but it must parse as an integer when the connector executes. |
| `kafka` | Object | Yes | Holds the `kafka.*` parameters in this table, plus optional consumer settings (`securityProtocol`, `metadataMaxAgeMs`, `consumerPropertyFile`, `consumer`); see [Security](#security). Other keys in it are not validated. |
| `idField` | String | No | JSON field in the Kafka message whose value becomes the Document ID. If omitted, or if a message lacks this field, the message's `id` field is used. |
| `kafka.documentDeserializer` | String | No | Fully-qualified class name of a custom Kafka `Deserializer<Document>`. Its `configure()` receives the consumer properties plus `idField` (if set) and `docIdPrefix`. Default: `com.kmwllc.lucille.connector.KafkaConnectorDefaultDeserializer`. |
| `maxMessages` | Long | No | Maximum number of messages to publish before stopping. If omitted, there is no limit. |
| `messageTimeout` | Long | No | Kafka poll timeout in milliseconds. Default: `100`. |
| `offsets` | Map\<Integer, Long\> | No | Map of partition numbers to starting offsets, for example `offsets { "0": 100 }`. Each listed partition is sought to its offset the first time it is assigned to the consumer; after a later rebalance it continues from the committed offset. Partitions not listed, or all partitions if `offsets` is omitted, use the consumer group's committed offsets. |
| `continueOnTimeout` | Boolean | No | If `true`, keep polling when a poll returns no messages. If `false`, the connector stops on the first empty poll, which can happen before the consumer has joined its group and read anything. Default: `true`. |

With the defaults (`continueOnTimeout: true` and no `maxMessages`), the connector never finishes on its own: it runs until it is interrupted or until `runner.connectorTimeout` (default 24 hours) elapses, which fails the run.

## Consumer Behavior

- Offsets are committed automatically in the background (`enable.auto.commit` is `true`), independently of whether the published documents have been processed or indexed. After a crash or timeout, messages that were polled but not yet indexed may not be read again on the next run. This differs from Lucille's own Kafka messaging in distributed mode, which commits only after a batch completes.
- With no committed offset for the group, consumption starts from the earliest offset (`auto.offset.reset` is `earliest`).
- Each poll returns at most one record (`max.poll.records` is `1`).
- Message keys are read as strings and are not used.

## Message Format

With the default deserializer, each Kafka message value must be a JSON object. Each JSON object becomes a Lucille Document, and its field names map directly to Document field names.

The Document ID is taken from `idField` if it is set and present in the message, otherwise from the message's `id` field. Non-string values are converted to text (for example `42` becomes `"42"`). The connector's `docIdPrefix` is prepended. If the value of `idField` is used, it is written to `id` and the original field is kept. A message with neither field (a JSON `null` counts as missing), an ID that converts to an empty or blank string (such as an object or array, when no `docIdPrefix` is set), or a value that is not a JSON object fails deserialization. A message with a null value (a Kafka tombstone) fails when it is published.

Either failure stops the connector and fails the run. The consumer's position does not move past a record that fails deserialization, so the next run with the same consumer group hits the same record again. The `kafka.onDeserializationError` skip mode used by Lucille's Workers and Indexer in distributed mode does not apply to this connector: the key is accepted under `kafka` but ignored.

Example Kafka message:
```json
{
  "article_id": "art-001",
  "title": "Breaking News",
  "body": "Full article text...",
  "published_at": "2025-06-01T12:00:00Z"
}
```

## Security

The connector builds its Kafka consumer only from its own connector config. It does not read the top-level `kafka {}` block. Besides the parameters above, the connector's `kafka` block accepts the consumer settings that Lucille's distributed messaging uses:

- `kafka.securityProtocol`: sets the consumer's `security.protocol`.
- `kafka.metadataMaxAgeMs`: sets `metadata.max.age.ms`. Default: `30000`.
- `kafka.consumerPropertyFile`: path to a Java properties file used as the base consumer properties instead of the generated ones (`bootstrapServers`, `consumerGroupId`, `securityProtocol`, `maxPollIntervalSecs`, `metadataMaxAgeMs` and the `auto.offset.reset`, `max.poll.records` and string key deserializer defaults described above are then not applied, so the file must set `key.deserializer` itself). `kafka.bootstrapServers`, `kafka.consumerGroupId` and `kafka.maxPollIntervalSecs` are still required but have no effect.
- `kafka.consumer`: arbitrary Kafka consumer properties, merged over the above, for example TLS or SASL settings. Dotted property names may be quoted (`"ssl.truststore.location": ...`) or unquoted; setting the same property both ways with different values fails when the connector executes, and a list value is rejected.

`enable.auto.commit`, the value deserializer, `idField` and `docIdPrefix` are always set by the connector after these are applied. Other keys under `kafka` are accepted without validation, so a misspelled key is silently ignored.

## Kafka as the Messaging Layer vs. as a Source

These are two distinct uses of Kafka in Lucille:

| Role | Description | Configuration |
|---|---|---|
| **Source (KafkaConnector)** | Reads application data from a Kafka topic. | Use `KafkaConnector` in your connectors list. |
| **Messaging layer** | Carries Documents between Lucille components in distributed mode. | Add `-distributed` flag to the Runner; configure the `kafka {}` block. |

Both can be active simultaneously: a `KafkaConnector` reads data from one topic while Lucille's distributed messaging uses separate internal topics.

## Related

- [Topology: Fully Distributed]({{< relref "docs/architecture/overview/topology" >}})
- [Topology: Connector-less Distributed]({{< relref "docs/architecture/overview/topology" >}})
- [Getting Started: Distributed Mode]({{< relref "docs/getting-started" >}})
