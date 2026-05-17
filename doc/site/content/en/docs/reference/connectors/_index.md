---
title: Connectors
weight: 1
date: 2024-10-15
description: Catalogue of built-in connectors and file handlers shipped with Lucille.
---

For conceptual documentation — what a Connector is, the Connector lifecycle, common configuration parameters, and how to build a custom Connector — see [Architecture: Connectors]({{< relref "docs/architecture/components/Connectors" >}}).

## Lucille Connectors (Core)

| Connector | Description |
|---|---|
| [File Connector]({{< relref "docs/reference/connectors/file_connector" >}}) | Traverses local, S3, Azure, or GCS file systems and publishes documents. Supports CSV, JSON, XML file handlers, incremental mode, and tombstone deletions. |
| [Database Connector]({{< relref "docs/reference/connectors/database_connector" >}}) | Reads rows from any JDBC-compatible database. |
| [Kafka Connector]({{< relref "docs/reference/connectors/kafka_connector" >}}) | Reads documents from a Kafka topic as a data source. |
| [RSS Connector]({{< relref "docs/reference/connectors/rss_connector" >}}) | Publishes documents from an RSS feed, with optional incremental refresh. |
| [Sequence Connector](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/SequenceConnector.java) *(source only)* | Generates a configurable number of empty Documents. Useful for testing. Requires `numDocs`; accepts optional `startWith`. |
| [Solr Connector]({{< relref "docs/reference/connectors/solr_connector" >}}) | Reads documents from a Solr collection using cursor-based pagination. Supports pre/post update actions. |

**The following connectors are deprecated.** Use FileConnector with a corresponding FileHandler instead.

| Connector | Replacement |
|---|---|
| [CSV Connector *(Deprecated)*](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/CSVConnector.java) | FileConnector with `csv` FileHandler |
| [JSON Connector *(Deprecated)*](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/JSONConnector.java) | FileConnector with `json` FileHandler |
| [XML Connector *(Deprecated)*](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/xml/XMLConnector.java) | FileConnector with `xml` FileHandler |

## Lucille Connectors (Plugins)

| Connector | Description |
|---|---|
| [Parquet Connector]({{< relref "docs/reference/connectors/parquet_connector" >}}) | Reads Apache Parquet files and publishes each row as a Document. Requires `lucille-parquet` dependency. |

File Handler configuration (CSV, JSON, XML, custom) is documented on the [File Connector]({{< relref "docs/reference/connectors/file_connector" >}}) page.
