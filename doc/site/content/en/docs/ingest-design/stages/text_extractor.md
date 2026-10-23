---
title: TextExtractor
weight: 610
date: 2025-06-09
description: Extracts text and metadata from file contents or file paths using Apache Tika.
---

`com.kmwllc.lucille.tika.stage.TextExtractor` *(requires `lucille-tika`)*

Extracts text and metadata from files, such as PDF, Microsoft Office and HTML documents, using [Apache Tika](https://tika.apache.org/). The input is either the bytes in `byteArrayField` or the file at the path in `filePathField` (a local path, a `classpath:` path, or a cloud storage URI). Exactly one of the two must be set, or the stage fails when it is created (including during pre-run validation). Documents without the configured input field are passed through unchanged. Only the first value of `filePathField` is used.

The extracted text is added to `textField`; if the field already has a value, the text is added as another value, making the field multi-valued. If a file cannot be read, a warning is logged and the document continues through the pipeline. If Tika fails to parse it, a warning is logged, whatever text was extracted (possibly an empty string) and metadata are still added, and the document continues. When `fork.enabled` is `true`, a parse failure instead throws an exception for that document.

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
  byteArrayField: "file_content"
  textField: "extracted_text"
}
```

| Parameter | Type | Required | Description |
|---|---|---|---|
| `byteArrayField` | String | No | Field containing the file contents as bytes. Set this or `filePathField`, not both. |
| `filePathField` | String | No | Field containing the path or URI of the file to read. Set this or `byteArrayField`, not both. |
| `textField` | String | No | Field the extracted text is added to. Default: `text`. |
| `metadataPrefix` | String | No | Prefix for metadata field names. With an empty string, metadata fields have no prefix. Default: `tika`. |
| `whitelist` | List\<String\> | No | Metadata names to keep, in cleaned form and without the prefix. If set, all other metadata is dropped. |
| `blacklist` | List\<String\> | No | Metadata names to drop, in cleaned form and without the prefix. |
| `fieldNamesField` | String | No | If set, the name of each metadata field written to the Document is added to this multi-valued field. |
| `replaceDotsInMetadataNames` | Boolean | No | Replace `.` with `_` in metadata names. See [Metadata fields](#metadata-fields). Default: `false`. |
| `metadataFields` | Map\<String, String\> | No | Input metadata hints for Tika. See [Detection hints](#detection-hints). |
| `textContentLimit` | Integer | No | Maximum number of characters of text to extract. Default: no limit. |
| `parseTimeout` | Long | No | Parse timeout in milliseconds. When a parse times out, a warning is logged and whatever text was extracted so far is kept. Default: no timeout. |
| `enableOcr` | Boolean | No | OCR images and image-only PDF pages. See [OCR](#ocr). Cannot be set together with `tikaConfigPath`. Default: `false`. |
| `tikaConfigPath` | String | No | Local path to a Tika config file. The config is used as is, and is applied to embedded documents as well as the top-level document. |
| `skipEmbeddedContentTypePrefixes` | List\<String\> | No | Content-type prefixes for which embedded documents are skipped (not parsed), e.g. `["image/"]`. Embedded parts with no content type are always parsed. Cannot be set when `fork.enabled` is `true`. Default: empty (all embedded documents are parsed). |
| `fork.enabled` | Boolean | No | Parse in child JVMs with Tika's `ForkParser`. This adds overhead per document but keeps crashes such as out-of-memory errors out of the Lucille JVM. When enabled, `parseTimeout` is passed to the `ForkParser` as its parse timeout. Default: `false`. |
| `fork.poolSize` | Integer | No | Number of child JVMs kept in the pool. Default: `5`. |
| `fork.jvmArgs` | List\<String\> | No | JVM arguments for each child JVM. `java` is prepended automatically. Default: `["java", "-Djava.awt.headless=true"]`. |
| `fork.serverPulseMillis` | Long | No | Heartbeat interval in milliseconds between the parent and child JVMs. Default: `1000`. |
| `fetcherClass` | String | No | Class name of a custom `FileContentFetcher` used to read `filePathField` paths. It must have a public constructor that takes a `Config`. Default: Lucille's built-in fetcher. |
| `s3` | Object | No | S3 credentials for reading `filePathField` paths from S3: `accessKeyId`, `secretAccessKey`, `region`, `anonymous`, `maxResultsPerPage`. See [Cloud Storage Configuration]({{< relref "docs/ingest-design/connectors/file_connector#cloud-storage-configuration" >}}). |
| `azure` | Object | No | Azure credentials for reading `filePathField` paths from Azure: `connectionString`, `accountName`, `accountKey`, `maxResultsPerPage`. See [Cloud Storage Configuration]({{< relref "docs/ingest-design/connectors/file_connector#cloud-storage-configuration" >}}). |
| `gcp` | Object | No | Google Cloud credentials for reading `filePathField` paths from GCS: `pathToServiceKey` (required within `gcp`), `maxResultsPerPage`. See [Cloud Storage Configuration]({{< relref "docs/ingest-design/connectors/file_connector#cloud-storage-configuration" >}}). |

## OCR

OCR is off by default. Set `enableOcr: true` to OCR images and image-only PDF pages through Tika's Tesseract parser, which requires a `tesseract` binary on the `PATH` and is much slower than plain text extraction. Earlier versions ran OCR automatically whenever `tesseract` was installed; set `enableOcr: true` to keep that behavior.

A custom Tika config is used as is, so `enableOcr` cannot be set (even to `false`) together with `tikaConfigPath`; the stage fails when it is created. Control OCR in the Tika config instead, for example with `<parser-exclude class="org.apache.tika.parser.ocr.TesseractOCRParser"/>` under the `DefaultParser`. A global Tika config set with the `tika.config` system property or the `TIKA_CONFIG` environment variable is treated the same way when `tikaConfigPath` is not set. At startup the stage logs whether OCR is enabled and whether `tesseract` was found.

## Metadata fields

Each metadata value Tika returns is added to a field named `<metadataPrefix>_<name>`. The name is trimmed and lowercased, and spaces, `-` and `:` become `_`. `whitelist` and `blacklist` match this cleaned name, without the prefix. When both are set, a name must be in `whitelist` and not in `blacklist`.

Tika also emits dotted names such as `dc.date` and `dc.date.created`. OpenSearch and Elasticsearch read a dotted field name as an object path and reject documents carrying both. When indexing into either, set `replaceDotsInMetadataNames: true` to replace `.` with `_` as well (off by default, so existing field names are unchanged). It applies to metadata names only, not to `metadataPrefix`. With it on, `whitelist`/`blacklist` entries must use the replaced form (dotted entries never match; a warning is logged), and names that become equal (e.g. `dc.date` and `dc_date`) are merged into one multi-valued field.

## Detection hints

`metadataFields` maps Tika metadata keys to Document fields. Before parsing, the values of each field are added to the metadata handed to Tika, which uses them to detect the content type. Fields missing from the Document are skipped. These input values are also written back to the Document with the extracted metadata, subject to `whitelist` and `blacklist`. The most useful keys are `resourceName` (the file name, so Tika can use the extension) and `Content-Type` (an advertised content type, such as one returned by a web server).

```hocon
metadataFields: {
  resourceName: "file_name"
  "Content-Type": "content_type"
}
```

## Performance

Tika can be an intensive process. There are some small alterations we have made within the stage to improve its throughput; if
you are working closely with `TextExtractor` and focused on performance, you may want to look into these a bit closer.
