---
title: TextExtractor
weight: 610
date: 2025-06-09
description: Extracts text from over 1,000 file formats using Apache Tika.
---

`com.kmwllc.lucille.tika.stage.TextExtractor` *(requires `lucille-tika`)*

Extracts text from over 1,000 file formats — PDF, Microsoft Office documents, HTML, images (with optional OCR), and many more — using [Apache Tika](https://tika.apache.org/). Reads a file (from a path or raw bytes on the document) and writes the extracted text and metadata back onto the document.

Provide exactly one of `filePathField` or `byteArrayField` as the source.

**OCR is off by default.** Set `enableOcr: true` to OCR images and image-only PDF pages through Tika's Tesseract parser, which requires a `tesseract` binary on the `PATH` and is much slower than plain text extraction. Earlier versions ran OCR automatically whenever `tesseract` was installed; set `enableOcr: true` to keep that behavior. `enableOcr` cannot be combined with `tikaConfigPath` — a custom Tika config is used as is, so control OCR there instead (for example, `<parser-exclude class="org.apache.tika.parser.ocr.TesseractOCRParser"/>` under the `DefaultParser`). A global Tika config set with the `tika.config` system property or the `TIKA_CONFIG` environment variable is treated the same way when `tikaConfigPath` is not set. At startup the stage logs whether OCR is enabled and whether `tesseract` was found.

**Maven dependency:**
```xml
<dependency>
  <groupId>com.kmwllc</groupId>
  <artifactId>lucille-tika</artifactId>
  <version>${lucille.version}</version>
</dependency>
```

| Parameter | Type | Required | Description |
|---|---|---|---|
| `filePathField` | String | One of `filePathField`/`byteArrayField` | Document field holding the path to the file to parse. |
| `byteArrayField` | String | One of `filePathField`/`byteArrayField` | Document field holding the file's raw bytes to parse. |
| `textField` | String | No | Destination field for the extracted text. Defaults to `text`. |
| `metadataPrefix` | String | No | Prefix applied to fields holding extracted metadata. Defaults to `tika`. |
| `textContentLimit` | Integer | No | Maximum number of characters of extracted text; text beyond this is truncated. Defaults to unlimited. |
| `parseTimeout` | Long | No | Timeout for parsing a single document, in milliseconds. |
| `tikaConfigPath` | String | No | Path to a Tika config file, used as is. If omitted, a default `AutoDetectParser` is used (without OCR unless `enableOcr` is true). A provided config is applied to embedded documents as well as the top-level document. |
| `enableOcr` | Boolean | No | Whether to OCR images and image-only PDF pages with Tika's Tesseract parser. Defaults to `false`. Cannot be combined with `tikaConfigPath` or a global Tika config. |
| `skipEmbeddedContentTypePrefixes` | List\<String\> | No | Content-type prefixes for which embedded documents are skipped (not parsed), e.g. `["image/"]`. Defaults to empty (all embedded documents are parsed). Embedded parts with no content type are always parsed. Not supported with `fork`. |
| `whitelist` | List\<String\> | No | Metadata names to include in the document. |
| `blacklist` | List\<String\> | No | Metadata names to exclude from the document. |
| `replaceDotsInMetadataNames` | Boolean | No | Replace `.` with `_` in metadata names. Defaults to `false`. See below. |
| `fieldNamesField` | String | No | If set, each extracted metadata field's prefixed name is added as a value to this multi-valued field. |
| `metadataFields` | Map | No | Maps Tika metadata keys to document field names whose values are supplied to Tika as detection hints before parsing (e.g. `resourceName`, `Content-Type`). |
| `fork` | Map | No | Runs parsing in a child JVM via Tika's `ForkParser` to isolate crashes. Keys: `enabled` (Boolean), `poolSize` (Integer), `jvmArgs` (List\<String\>), `serverPulseMillis` (Long). |
| `s3` / `azure` / `gcp` | Map | No | Credentials for reading `filePathField` files from cloud storage. See the [File Connector]({{< relref "docs/ingest-design/connectors/file_connector" >}}) for the arguments. |

```hocon
{
  name: "extract-text"
  class: "com.kmwllc.lucille.tika.stage.TextExtractor"
  byteArrayField: "file_content"
  textField: "extracted_text"
}
```

Extracted Tika metadata is written to fields named `<metadataPrefix>_<name>`, where the name is lowercased and spaces, `-` and `:` become `_`. Tika also emits dotted names such as `dc.date` and `dc.date.created`; OpenSearch and Elasticsearch read a dotted field name as an object path and reject documents carrying both. When indexing into either, set `replaceDotsInMetadataNames: true` to replace `.` with `_` as well (off by default, so existing field names are unchanged). It applies to metadata names only, not to `metadataPrefix`. With it on, `whitelist`/`blacklist` entries must use the replaced form (dotted entries never match; a warning is logged), and names that become equal (e.g. `dc.date` and `dc_date`) are merged into one multi-valued field.

Tika can be an intensive process. There are some small alterations we have made within the stage to improve its throughput; if
you are working closely with `TextExtractor` and focused on performance, you may want to look into these a bit closer.
