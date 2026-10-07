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

Extracted Tika metadata is written to fields named `<metadataPrefix>_<name>`, where the name is lowercased and spaces, `-` and `:` become `_`. Tika also emits dotted names such as `dc.date` and `dc.date.created`; OpenSearch and Elasticsearch read a dotted field name as an object path and reject documents carrying both. When indexing into either, set `replaceDotsInMetadataNames: true` to replace `.` with `_` as well (off by default, so existing field names are unchanged). It applies to metadata names only, not to `metadataPrefix`. With it on, `whitelist`/`blacklist` entries must use the replaced form (dotted entries never match; a warning is logged), and names that become equal (e.g. `dc.date` and `dc_date`) are merged into one multi-valued field.
