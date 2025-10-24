---
title: Connectors
date: 2024-10-15
description: A component that retrieves data from a source system and packages the data into Documents in preparation for transformation.
---

## Lucille Connectors

Lucille Connectors are components that retrieve data from a source system, packages the data into "Documents", and publishes them to a pipeline.

To configure a Connector, you have to provide its class (under `class`) in its config. You also need to specify a `name` for the Connector.
Optionally, you can specify the `pipeline`, a `docIdPrefix`, and whether the Connector requires a Publisher to `collapse`.

You'll also provide the parameters needed by the Connector as well. For example, the `SequenceConnector` requires one parameter, `numDocs`,
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

### Lucille Connectors (Core)

* [Abstract Connector](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/AbstractConnector.java) - Base implementation for a `Connector`.
* [Database Connector](database_connector.md)
* [File Connector](file_connector.md)
* [Sequence Connector](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/SequenceConnector.java) - Generates a certain number of empty Documents.
* [Solr Connector](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/SolrConnector.java)
* [RSS Connector](rss_connector.md)

**The following connectors are deprecated.** Use FileConnector instead, along with a corresponding FileHandler.

* [CSV Connector **(Deprecated)**](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/CSVConnector.java)
* [JSON Connector **(Deprecated)**](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/JSONConnector.java)
* [XML Connector **(Deprecated)**](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/xml/XMLConnector.java)

### Lucille Connectors (Plugins)

* [Parquet Connector](https://github.com/kmwtechnology/lucille/blob/main/lucille-plugins/lucille-parquet/src/main/java/com/kmwllc/lucille/parquet/connector/ParquetConnector.java)