---
title: ApplyOCR
weight: 110
date: 2025-06-09
description: Performs optical character recognition on image fields using Tesseract.
---

`com.kmwllc.lucille.ocr.stage.ApplyOCR` *(requires `lucille-ocr`)*

Performs optical character recognition on image fields using [Tesseract](https://tesseract-ocr.github.io/). Reads image bytes from a source field and writes the recognized text to a destination field. Requires Tesseract to be installed on the system running Lucille.

**Maven dependency:**
```xml
<dependency>
  <groupId>com.kmwllc</groupId>
  <artifactId>lucille-ocr</artifactId>
  <version>${lucille.version}</version>
</dependency>
```

```hocon
{
  name: "ocr"
  class: "com.kmwllc.lucille.ocr.stage.ApplyOCR"
  source: "image_bytes"
  dest: "ocr_text"
  language: "eng"
}
```
