---
title: Connectors
date: 2024-10-15
description: A component that retrieves data from a source system, packages the data into  “documents,” and publishes them.
---

## Lucille Connectors

Lucille Connectors are components that retrieve data from a source system, packages the data into "documents," and publishes them to a pipeline.

The core Lucille project contains a number of commonly used connectors. Additional connectors that rely on a large number of dependencies are provided as optional plugin modules.

### Lucille Connectors (Core)

* [Abstract Connector](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/AbstractConnector.java)
* [Database Connector](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/jdbc/DatabaseConnector.java)
* [File Connector](file_connector.md)
* [Sequence Connector](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/SequenceConnector.java)
* [Solr Connector](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/SolrConnector.java)

**The following connectors are deprecated.** Use FileConnector instead, along with a corresponding FileHandler.

* [CSV Connector **(Deprecated)**](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/CSVConnector.java)
* [JSON Connector **(Deprecated)**](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/JSONConnector.java)
* [VFS Connector **(Deprecated)**](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/VFSConnector.java)
* [XML Connector **(Deprecated)**](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/xml/XMLConnector.java)

### Lucille Connectors (Plugins)

* [OCR Connector](https://github.com/kmwtechnology/lucille/tree/main/lucille-plugins/lucille-ocr)
* [Parquet Connector](https://github.com/kmwtechnology/lucille/blob/main/lucille-plugins/lucille-parquet/src/main/java/com/kmwllc/lucille/parquet/connector/ParquetConnector.java)
