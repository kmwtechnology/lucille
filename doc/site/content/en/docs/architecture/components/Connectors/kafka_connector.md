---
title: Kafka Connector
date: 2025-06-09
description: A Connector that reads Documents from a Kafka topic and publishes them into the Lucille pipeline.
---

[Source Code](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/KafkaConnector.java)

The `KafkaConnector` reads Documents from a Kafka topic and publishes them into a Lucille pipeline. This is distinct from Kafka's role as the *messaging layer* in distributed mode — the `KafkaConnector` is a data *source*, reading documents produced by an upstream system.

## Use Cases

- **Streaming ingest from Kafka:** An upstream application publishes documents (as JSON) to a Kafka topic, and Lucille reads them for enrichment and indexing.
- **Connectorless distributed mode:** In this deployment pattern, a third-party publisher puts documents onto a Kafka source topic, and Lucille Workers consume them directly. In this case the `KafkaConnector` is not used — Workers listen to the source topic directly.

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
    maxMessages: 10000
  }
]
```

## Configuration Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `kafka.bootstrapServers` | String | Yes | Comma-separated list of Kafka broker addresses. |
| `kafka.topic` | String | Yes | Kafka topic to consume from. |
| `kafka.consumerGroupId` | String | Yes | Consumer group ID. |
| `kafka.clientId` | String | Yes | Kafka client identifier for logging and monitoring. |
| `kafka.maxPollIntervalSecs` | Integer | Yes | Maximum time between Kafka polls before the consumer is evicted from the consumer group. |
| `idField` | String | No | JSON field in the Kafka message to use as the Document ID. If omitted, a UUID is generated. |
| `kafka.documentDeserializer` | String | No | Fully-qualified class name of a custom `Deserializer<Document>`. Defaults to the built-in JSON deserializer. |
| `maxMessages` | Long | No | Maximum number of messages to consume before stopping. If omitted, runs until no more messages are available. |
| `messageTimeout` | Long | No | Kafka poll timeout in milliseconds. Default: `100`. |
| `offsets` | Map\<Integer, Long\> | No | Map of partition numbers to starting offsets. If omitted, uses the consumer group's committed offset. |
| `continueOnTimeout` | Boolean | No | If `true`, continue polling after a poll timeout instead of stopping. |

## Message Format

The `KafkaConnector` expects each Kafka message value to be a JSON object. Each JSON object becomes a Lucille Document. Field names in the JSON map directly to Document field names.

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

For Kafka clusters with TLS or SASL authentication, use the top-level `kafka {}` block (separate from the connector's inline params) to provide properties files and security settings:

```hocon
kafka {
  bootstrapServers: "kafka1:9092"
  securityProtocol: "SSL"
  consumerPropertyFile: "/path/to/consumer.properties"
  producerPropertyFile: "/path/to/producer.properties"
  adminPropertyFile: "/path/to/admin.properties"
}
```

`securityProtocol`, `consumerPropertyFile`, `producerPropertyFile`, and `adminPropertyFile` are properties of the top-level `kafka {}` block and apply to all Kafka communication in the process, not just the `KafkaConnector`.

## Kafka as the Messaging Layer vs. as a Source

These are two distinct uses of Kafka in Lucille:

| Role | Description | Configuration |
|---|---|---|
| **Source (KafkaConnector)** | Reads application data from a Kafka topic. | Use `KafkaConnector` in your connectors list. |
| **Messaging layer** | Carries Documents between Lucille components in distributed mode. | Add `-useKafka` flag to the Runner; configure the `kafka {}` block. |

Both can be active simultaneously: a `KafkaConnector` reads data from one topic while Lucille's distributed messaging uses separate internal topics.

## Related

- [Topology: Fully Distributed]({{< relref "docs/architecture/topology" >}})
- [Topology: Connector-less Distributed]({{< relref "docs/architecture/topology" >}})
- [Getting Started: Distributed Mode]({{< relref "docs/getting-started" >}})
