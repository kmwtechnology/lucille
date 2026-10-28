---
title: Connectors
weight: 3
date: 2024-10-15
description: Catalogue of built-in connectors and how to configure them.
---

For conceptual documentation — what a Connector is, the lifecycle design, and how Connectors are decoupled from downstream components — see [Architecture: Connector]({{< relref "docs/architecture/components/connector" >}}).

## Configuring a Connector

To configure a Connector, provide its `class` and (recommended) a `name` in the config. Optionally specify the `pipeline` it feeds, a `docIdPrefix` for ID namespacing, and whether it requires document collapsing:

```hocon
{
  name: "my-connector"
  class: "com.kmwllc.lucille.connector.FileConnector"
  pipeline: "my-pipeline"
  docIdPrefix: "files-"
  paths: ["/data/files"]
}
```

### Common Parameters

These parameters are available on all Connectors via `AbstractConnector`:

| Parameter | Type | Required | Description |
|---|---|---|---|
| `class` | String | Yes | Fully qualified class name of the Connector implementation. Not enforced by the Spec, but the Runner cannot load the connector without it. |
| `name` | String | No | Connector name for logging and run summaries. Two connectors with the same name fail at startup. Default: `connector_<N>`, where N is the connector's 1-based position in `connectors`. |
| `pipeline` | String | No | Name of the pipeline to process this connector's documents. It must match the `name` of a pipeline in `pipelines`, or pre-run validation fails. If omitted, no Workers, Indexer or Publisher are started, and the connector runs with no Publisher, so a connector that publishes documents fails. Only use this for connectors that do their work outside of publishing (for example `SolrConnector` actions or `SleepConnector`). See [Setup-Only Connectors]({{< relref "docs/ingest-design/control-flow#setup-only-connectors-no-pipeline" >}}) for this pattern. |
| `docIdPrefix` | String | No | String prepended to Document IDs to prevent collisions across connectors. Applied only by connectors that build IDs with it: the Database, Kafka (default deserializer), Solr, Sequence and File connectors, and the deprecated CSV, JSON and XML connectors. The RSS and Parquet connectors ignore it. Default: no prefix. |
| `collapse` | Boolean | No | If `true`, the Publisher merges consecutive published documents that share the same ID into one document before sending it for processing (see [Collapsing](#collapsing)). Default: `false`. |

### Collapsing

With `collapse: true`, the Publisher holds each published document until it sees a document with a different ID (or the connector finishes). Each following document with the same ID is merged into the held one: every field it carries is appended to the held document's field, which becomes multi-valued (a field the held document lacks is copied as is). Reserved fields (`id`, `run_id`, children and the drop/skip markers) of the later documents are not copied, so their children are lost; the first document keeps its own. Only consecutive documents are merged; the same ID appearing later is sent as a separate document. A collapsing Publisher is not safe for concurrent `publish()` calls, so `FileConnector` rejects `collapse` together with concurrent traversal (`maxThreads` other than `1`) at startup.

### Sequencing Multiple Connectors

A single Lucille run can chain multiple Connectors in sequence. Each Connector runs to completion (all its documents processed and indexed) before the next begins, and if a Connector fails the remaining ones do not run:

```hocon
connectors: [
  { name: "parent-docs",  class: "...", pipeline: "pipeline1" },
  { name: "child-docs",   class: "...", pipeline: "pipeline1" }
]
```

---

## Lucille Connectors (Core)

| Connector | Description |
|---|---|
| [File Connector]({{< relref "docs/ingest-design/connectors/file_connector" >}}) | Traverses local, S3, Azure, or GCS file systems and publishes documents. Supports CSV, JSON, XML file handlers, incremental mode, and tombstone deletions. |
| [Database Connector]({{< relref "docs/ingest-design/connectors/database_connector" >}}) | Reads rows from any JDBC-compatible database. |
| [Kafka Connector]({{< relref "docs/ingest-design/connectors/kafka_connector" >}}) | Reads documents from a Kafka topic as a data source. |
| [RSS Connector]({{< relref "docs/ingest-design/connectors/rss_connector" >}}) | Publishes documents from an RSS feed, with optional incremental refresh. |
| [Sequence Connector](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/SequenceConnector.java) *(source only)* | Generates `numDocs` (required) empty Documents with numeric IDs `0`, `1`, ..., offset by `startWith` (integer, default `0`) and prefixed with `docIdPrefix`. Requires a `pipeline`. Useful for testing. |
| [Sleep Connector](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/SleepConnector.java) *(source only)* | Sleeps for `duration` (required) milliseconds and publishes nothing, so it can run without a `pipeline`. Useful for testing. |
| [Solr Connector]({{< relref "docs/ingest-design/connectors/solr_connector" >}}) | Reads documents from a Solr collection using cursor-based pagination. Supports pre/post update actions. |

**The following connectors are deprecated.** Use FileConnector with a corresponding FileHandler instead.

| Connector | Replacement |
|---|---|
| [CSV Connector *(Deprecated)*](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/CSVConnector.java) | FileConnector with `csv` FileHandler |
| [JSON Connector *(Deprecated)*](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/JSONConnector.java) | FileConnector with `json` FileHandler |
| [XML Connector *(Deprecated)*](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/xml/XMLConnector.java) | FileConnector with `xml` FileHandler |

## Lucille Connectors (Plugins)

| Connector | Description |
|---|---|
| [Parquet Connector]({{< relref "docs/ingest-design/connectors/parquet_connector" >}}) | Reads Apache Parquet files and publishes each row as a Document. Requires `lucille-parquet` dependency. |

File Handler configuration (CSV, JSON, XML, custom) is documented on the [File Connector]({{< relref "docs/ingest-design/connectors/file_connector" >}}) page.
