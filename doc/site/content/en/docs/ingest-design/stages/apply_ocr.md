---
title: ApplyOCR
weight: 110
date: 2025-06-09
description: Performs optical character recognition on image and PDF files using Tesseract, with optional template-based form extraction.
---

`com.kmwllc.lucille.ocr.stage.ApplyOCR` *(requires `lucille-ocr`)*

Performs optical character recognition on the image or PDF file at the local path in `pathField`, using [Tesseract](https://tesseract-ocr.github.io/). A file is treated as a PDF when its extension is `pdf`; each PDF page is rendered to an image at 300 DPI. Documents without `pathField` are skipped with a warning, and so are documents whose file cannot be loaded. Only the first value of `pathField` is used.

The stage can do full extraction (`extractAllDest`), form extraction with templates (`extractionTemplates` with `pages` and/or `pagesField`), or both.

At startup, Tesseract loads the trained data for `lang` from a `TesseractOcr` directory in the working directory Lucille is run from (for example `TesseractOcr/eng.traineddata`). The stage fails at startup if it cannot be loaded. Temporary images are written to a `lucille-ocr-temp` directory in the working directory.

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
  pathField: "file_path"
  lang: "eng"
  extractAllDest: "ocr_text"
}
```

| Parameter | Type | Required | Description |
|---|---|---|---|
| `lang` | String | Yes | Tesseract language code, such as `eng`. A matching `.traineddata` file must be in the `TesseractOcr` directory. |
| `pathField` | String | Yes | Field containing the local path of the image or PDF file. |
| `extractAllDest` | String | No | If set, the text of the whole file is written to this field, overwriting any existing value. For a PDF, each page becomes a separate value. |
| `extractionTemplates` | List\<Object\> | No | Form templates. See [Templates](#templates). Required if `pages` or `pagesField` is set; otherwise the stage fails at startup. |
| `pages` | Map\<String, String\> | No | Maps page numbers to template names, applied to every document. Pages are numbered from `0`; use `0` for a non-PDF image. |
| `pagesField` | String | No | Field containing a JSON string that maps page numbers to template names for that document, such as `{"0": "w2", "4": "w4"}`. Where a page appears in both `pages` and this field, this field wins. |

## Templates

Each template has a `name` and a list of `regions`. Each region is a rectangle given in pixels on the page image (`x`, `y`, `width`, `height`, all required) and a `dest` field (required). The text OCR'd from each region is appended to its `dest` field, so regions or templates that share a `dest` produce a multi-valued field. A page or template name that does not exist is logged and skipped.

```hocon
extractionTemplates: [
  {
    name: "w2"
    regions: [
      { x: 0, y: 0, width: 100, height: 100, dest: "field1" }
      { x: 100, y: 200, width: 200, height: 300, dest: "field2" }
    ]
  }
]
pages: { "0": "w2" }
```
