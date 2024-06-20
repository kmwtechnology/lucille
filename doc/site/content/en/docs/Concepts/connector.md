---
title: Connector
date: 2024-05-22
description: >
  A component that retrieves data from a source system, packages the data into  “documents,” and publishes them.
categories: [Examples]
tags: [test, sample, docs]
---

## Lucille Connectors

Lucille Connectors are components that retrieve data from a source system, packages the data into "documents," and publishes them to a pipeline.

The core Lucille project contains a number of commonly used connectors. Additional connectors that rely on a large number of dependencies are provided as optional plugin modules.

### Lucille Connectors (Core)

* [Abstract Connector](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/AbstractConnector.java)
* [CSV Connector](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/CSVConnector.java)
* [Database Connector](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/jdbc/DatabaseConnector.java)
* [JSON Connector](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/JSONConnector.java)
* [Sequence Connector](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/SequenceConnector.java)
* [Solr Connector](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/SolrConnector.java)
* [VFS Connector](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/VFSConnector.java)
* [XML Connector](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/xml/XMLConnector.java)

### Lucille Connectors (Plugins)

* [OCR Connector](https://github.com/kmwtechnology/lucille/tree/main/lucille-plugins/lucille-ocr)
* [Parquet Connector](https://github.com/kmwtechnology/lucille/blob/main/lucille-plugins/lucille-parquet/src/main/java/com/kmwllc/lucille/parquet/connector/ParquetConnector.java)
* [Text Extraction (Apache Tika) Connector](https://github.com/kmwtechnology/lucille/tree/main/lucille-plugins/lucille-tika) 
