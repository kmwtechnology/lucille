---
title: File Handlers
date: 2025-02-28
description: File Handlers extract Lucille Documents from individual files, like CSV or JSON files, which themselves contain data which can be transformed into Lucille Documents.
---

File Handlers accept an InputStream for processing, and return the Documents they extract in an Iterator. 
The provided InputStream and any other underlying resources are closed when the Iterator returns `false` for `hasNext()`. 
As such, when working directly with these File Handlers, it is important to exhaust the Iterators they return.

### File Handlers (Core)

* [CSV File Handler](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/core/fileHandler/CSVFileHandler.java): Extracts documents from a `csv` file.
* [JSON File Handler](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/core/fileHandler/JSONFileHandler.java): Extracts documents from a `json` (or a `jsonl`) file.
* [XML File Handler](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/core/fileHandler/XMLFileHandler.java): Extracts documents from an `xml` file.

### Custom File Handlers

Developers can implement and use custom File Handlers as needed. Extend `BaseFileHandler` to get started. To use a custom
`FileHandler`, you have to reference its `class` in its Config. This is not needed when using the File Handlers provided by Lucille.
You can override the File Handlers provided by Lucille, as well - just include the `class` you want to use in the Config.
