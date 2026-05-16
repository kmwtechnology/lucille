---
weight: 10
title: Connectors
date: 2024-10-15
description: A component that retrieves data from a source system and packages the data into Documents in preparation for transformation.
---

## Lucille Connectors

Lucille Connectors are components that retrieve data from a source system, package the data into "Documents", and publish them to a pipeline.

To configure a Connector, you have to provide its class (under `class`) in its config. You also need to specify a `name` for the Connector.
Optionally, you can specify the `pipeline`, a `docIdPrefix`, and whether the Connector requires a Publisher to `collapse`.

You'll also provide the parameters needed by the Connector. For example, the `SequenceConnector` requires one parameter, `numDocs`,
and accepts an optional parameter, `startWith`. So, a `SequenceConnector` Config would look something like this:

```hocon
{
  name: "Sequence-Connector-1"
  class: "com.kmwllc.lucille.connector.SequenceConnector"
  docIdPrefix: "sequence-connector-1-"
  pipeline: "pipeline1"
  numDocs: 500
  startWith: 50
}
```

The `lucille-core` module contains a number of commonly used connectors. Additional connectors with a large number of dependencies are provided as optional plugin modules.

## Common Configuration Parameters

These parameters are available on all Connectors via `AbstractConnector`:

| Parameter | Required | Description |
|---|---|---|
| `class` | Yes | Fully qualified class name of the Connector implementation. |
| `name` | Yes | Connector name for logging and run summaries. |
| `pipeline` | No | Name of the pipeline to process this connector's documents. If omitted, no Workers or Indexer are started for this connector and `execute()` is called synchronously with a `null` publisher — useful for connectors that perform side effects without producing documents. |
| `docIdPrefix` | No | String prefix prepended to every Document ID. |
| `collapse` | No | Whether the Publisher should collapse consecutive documents with the same ID (for CDC scenarios). Default: `false`. |

## Lucille Connectors (Core)

| Connector | Description |
|---|---|
| [File Connector]({{< relref "docs/architecture/components/Connectors/file_connector" >}}) | Traverses local, S3, Azure, or GCS file systems and publishes documents. Supports CSV, JSON, XML file handlers, incremental mode, and tombstone deletions. |
| [Database Connector]({{< relref "docs/architecture/components/Connectors/database_connector" >}}) | Reads rows from any JDBC-compatible database. |
| [Kafka Connector]({{< relref "docs/architecture/components/Connectors/kafka_connector" >}}) | Reads documents from a Kafka topic as a data source. |
| [RSS Connector]({{< relref "docs/architecture/components/Connectors/rss_connector" >}}) | Publishes documents from an RSS feed, with optional incremental refresh. |
| [Sequence Connector](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/SequenceConnector.java) *(source only)* | Generates a configurable number of empty Documents. Useful for testing. Requires `numDocs`; accepts optional `startWith`. |
| [Solr Connector]({{< relref "docs/architecture/components/Connectors/solr_connector" >}}) | Reads documents from a Solr collection using cursor-based pagination. Supports pre/post update actions. |

**The following connectors are deprecated.** Use FileConnector with a corresponding FileHandler instead.

| Connector | Replacement |
|---|---|
| [CSV Connector *(Deprecated)*](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/CSVConnector.java) | FileConnector with `csv` FileHandler |
| [JSON Connector *(Deprecated)*](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/JSONConnector.java) | FileConnector with `json` FileHandler |
| [XML Connector *(Deprecated)*](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/xml/XMLConnector.java) | FileConnector with `xml` FileHandler |

## Lucille Connectors (Plugins)

| Connector | Plugin | Description |
|---|---|---|
| [Parquet Connector](https://github.com/kmwtechnology/lucille/blob/main/lucille-plugins/lucille-parquet/src/main/java/com/kmwllc/lucille/parquet/connector/ParquetConnector.java) | lucille-parquet | Reads Apache Parquet files. |

See [Plugins]({{< relref "docs/architecture/components/plugins" >}}) for setup.

## Connector Lifecycle

Every Connector goes through four lifecycle phases on each run:

1. **`preExecute(runId)`** — Called before `execute`. Use for setup: acquiring locks, creating temporary tables, validating source accessibility.
2. **`execute(publisher)`** — The main phase. Read from the source and call `publisher.publish(doc)` for each Document.
3. **`postExecute(runId)`** — Called only if `execute` succeeds. Use for cleanup: releasing locks, writing completion markers.
4. **`close()`** — Always called, even on failure. Use for releasing resources.

## Sequencing Multiple Connectors

A single Lucille run can chain multiple Connectors in sequence. Each Connector runs to completion (all its documents processed and indexed) before the next begins:

```hocon
connectors: [
  { name: "parent-docs-connector",  class: "...", pipeline: "pipeline1" },
  { name: "child-docs-connector",   class: "...", pipeline: "pipeline1" }
]
```

This ordering guarantee is enforced automatically by the Publisher's accounting system, without external orchestration.

## Building a Custom Connector

See [Developing New Components]({{< relref "docs/developer-guide/dev_new_components" >}}) for a step-by-step guide and skeleton code.
