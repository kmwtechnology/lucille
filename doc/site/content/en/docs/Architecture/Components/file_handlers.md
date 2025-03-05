---
title: File Handlers
date: 2025-02-28
description: File Handlers extract Lucille documents from individual files, like CSV or JSON files, which themselves contain data which can be transformed into Lucille Documents.
---

File Handlers accept an InputStream for processing, and return the Documents they extract in an Iterator. 
The provided InputStream and any other underlying resources are closed when the Iterator returns `false` for `hasNext()`. 
As such, when working directly with these File Handlers, it is important to exhaust the Iterators they return.

### File Handlers (Core)

* [CSV File Handler](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/core/fileHandler/CSVFileHandler.java): Extracts documents from a `csv` file.
* [JSON File Handler](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/core/fileHandler/JSONFileHandler.java): Extracts documents from a `json` (or a `jsonl`) file.
* [XML File Handler](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/core/fileHandler/XMLFileHandler.java): Extracts documents from an `xml` file.

### File Handlers (Plugins)

* [Parquet File Handler](): Coming soon! Extracts documents from a `parquet` file.
