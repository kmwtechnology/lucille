---
title: TextExtractor
weight: 610
date: 2025-06-09
description: Extracts text from over 1,000 file formats using Apache Tika.
---

`com.kmwllc.lucille.tika.stage.TextExtractor` *(requires `lucille-tika`)*

Extracts text from over 1,000 file formats — PDF, Microsoft Office documents, HTML, images (with optional OCR), and many more — using [Apache Tika](https://tika.apache.org/). Reads raw bytes from a source field and writes extracted text to a destination field.

**OCR is off by default.** Set `enableOcr: true` to OCR images and image-only PDF pages through Tika's Tesseract parser, which requires a `tesseract` binary on the `PATH` and is much slower than plain text extraction. Earlier versions ran OCR automatically whenever `tesseract` was installed; set `enableOcr: true` to keep that behavior. `enableOcr` cannot be combined with `tikaConfigPath` — a custom Tika config is used as is, so control OCR there instead (for example, `<parser-exclude class="org.apache.tika.parser.ocr.TesseractOCRParser"/>` under the `DefaultParser`). A global Tika config set with the `tika.config` system property or the `TIKA_CONFIG` environment variable is treated the same way when `tikaConfigPath` is not set. At startup the stage logs whether OCR is enabled and whether `tesseract` was found.

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
