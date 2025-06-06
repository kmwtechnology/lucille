---
title: Connectors
date: 2024-10-15
description: A component that retrieves data from a source system, packages the data into  “documents,” and publishes them.
---

## Lucille Connectors

Lucille Connectors are components that retrieve data from a source system, packages the data into "documents," and publishes them to a pipeline.

The core Lucille project contains a number of commonly used connectors. Additional connectors that have a large number of dependencies are provided as optional plugin modules.

### Lucille Connectors (Core)

* [Abstract Connector](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/AbstractConnector.java)
* [Database Connector](database_connector.md)
* [File Connector](file_connector.md)
* [Sequence Connector](sequence_connector.md)
* [Solr Connector](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/SolrConnector.java)
* [RSS Connector](rss_connector.md) - Coming Soon!

**The following connectors are deprecated.** Use FileConnector instead, along with a corresponding FileHandler.

* [CSV Connector **(Deprecated)**](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/CSVConnector.java)
* [JSON Connector **(Deprecated)**](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/JSONConnector.java)
* [VFS Connector **(Deprecated)**](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/VFSConnector.java)
* [XML Connector **(Deprecated)**](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/xml/XMLConnector.java)

### Lucille Connectors (Plugins)

* [Parquet Connector](https://github.com/kmwtechnology/lucille/blob/main/lucille-plugins/lucille-parquet/src/main/java/com/kmwllc/lucille/parquet/connector/ParquetConnector.java)
