---
title: TextExtractor
weight: 610
date: 2025-06-09
description: Extracts text from over 1,000 file formats using Apache Tika.
---

`com.kmwllc.lucille.tika.stage.TextExtractor` *(requires `lucille-tika`)*

Extracts text from over 1,000 file formats — PDF, Microsoft Office documents, HTML, images with embedded OCR, and many more — using [Apache Tika](https://tika.apache.org/). Reads raw bytes from a source field and writes extracted text to a destination field.

**Maven dependency:**
```xml
<dependency>
  <groupId>com.kmwllc</groupId>
  <artifactId>lucille-tika</artifactId>
  <version>${lucille.version}</version>
</dependency>
```

```hocon
{
  name: "extract-text"
  class: "com.kmwllc.lucille.tika.stage.TextExtractor"
  source: "file_content"
  dest: "extracted_text"
}
```
