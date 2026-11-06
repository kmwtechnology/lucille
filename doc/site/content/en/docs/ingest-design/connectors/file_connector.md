---
title: File Connector
weight: 1
date: 2025-02-28
description: A Connector that traverses local filesystems and cloud storage (S3, Azure, GCS), applies pluggable file handlers, and publishes Lucille documents. Supports incremental mode, tombstone deletions, and archive unpacking.
---

[Source Code](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/FileConnector.java)

`com.kmwllc.lucille.connector.FileConnector`

The `FileConnector` traverses one or more paths and publishes a Lucille Document for each file it encounters. It supports local filesystems, Amazon S3, Azure Blob Storage, and Google Cloud Storage. A single connector config can traverse paths across multiple providers. Optional File Handlers turn files that contain records (CSV rows, JSON lines, XML elements) into one Document per record instead of one Document per file.

---

## Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `paths` | List\<String\> | Yes | Paths or URIs to traverse. See [Paths](#paths). |
| `maxThreads` | Integer | No | Number of threads used to traverse `paths`. `1` traverses them in order. A larger value traverses up to that many paths at once, with the rest waiting for a free thread, and `-1` uses one thread per path. Any other value fails at startup. With any value other than `1`, paths on the same storage provider must not overlap, and `collapse` cannot be used (even with a single path); both are checked at startup. Cloud paths overlap when one is a prefix of the other, so `s3://bucket/logs` and `s3://bucket/logs-old` overlap but `s3://bucket/logs/` and `s3://bucket/logs-old/` don't (see [Paths](#paths)). Overlap through symlinks or S3 access point aliases is not detected. If one traversal fails, the others still run to completion before the run fails. See [Concurrent Traversal](#concurrent-traversal). Default: `1`. |
| `filterOptions` | Object | No | Which files are processed. See [Filter Options](#filter-options). |
| `fileOptions` | Object | No | How files are handled. See [File Options](#file-options). |
| `state` | Object | No | JDBC-backed state for incremental runs. See [Incremental Mode and State](#incremental-mode-and-state). |
| `fileHandlers` | Object | No | File Handler configs, keyed by file extension. See [File Handlers](#file-handlers). |
| `gcp` | Object | No | Google Cloud Storage credentials. Required to traverse `gs://` paths. See [Cloud Storage Configuration](#cloud-storage-configuration). |
| `azure` | Object | No | Azure Blob Storage credentials. Required to traverse Azure paths. |
| `s3` | Object | No | Amazon S3 credentials and options. Required to traverse `s3://` paths. |
| `storageClients` | Object | No | Custom storage clients, keyed by URI scheme, for new schemes or in place of a built-in client. See [Custom Storage Clients](#custom-storage-clients). |

### Paths

The URI scheme of each path selects the storage backend:

| Storage | Path format |
|---|---|
| Local | `file:///path/to/dir`, or a plain path such as `/data`, `data/files`, `C:/data` or `C:\\data` (backslashes must be escaped in HOCON). |
| Amazon S3 | `s3://bucket/prefix` |
| Google Cloud Storage | `gs://bucket/prefix` |
| Azure Blob Storage | `https://{account}.blob.core.windows.net/{container}/prefix`. Other hosts need [`azure.allowedHosts`](#azure-hosts). |
| Other | Any scheme registered in [`storageClients`](#custom-storage-clients), e.g. `sftp://host/path`. |

A local filesystem client is always available. A path whose provider block (`s3`, `gcp`, `azure`) is missing fails the run when the connector reaches it. An `https` path whose host isn't in `azure.allowedHosts` fails at startup. With `maxThreads: 1` (the default), paths are traversed in order, and a path that fails stops the run before the remaining paths are traversed.

On S3, GCS, and Azure, a path is a raw key prefix, so `s3://bucket/data` also traverses keys under `data2/`. End the path with `/` to limit it to one folder. For S3 paths, percent-encode special characters (e.g., `s3://bucket/folder%20with%20spaces`). When no `pathsToSkip` filter is set, S3 paths are listed in one flat listing rather than directory by directory, so files are published in S3 key order (see [Large Traversals](#large-traversals)).

```hocon
paths: ["file:///local/data", "s3://my-bucket/prefix", "https://myaccount.blob.core.windows.net/container/path"]
```

---

## Cloud Storage Configuration

When traversing cloud storage, provide credentials in a block named `s3`, `gcp`, or `azure` inside the connector config. Each provider also accepts `maxResultsPerPage`, the number of results returned per listing request (default 100; 1000 for S3, which is also the most S3 returns per request), and the settings in [Large Traversals](#large-traversals).

### azure

| Parameter | Type | Required | Description |
|---|---|---|---|
| `connectionString` | String | No | Azure Storage connection string. |
| `accountName` | String | No | Storage account name. Used with `accountKey`. |
| `accountKey` | String | No | Storage account key. Used with `accountName`. |
| `maxResultsPerPage` | Integer | No | Number of results returned per listing request (page size). Default: `100`. |
| `allowedHosts` | List\<String\> | No | Hosts an `https` URI must contain to be read with the Azure client. See [Azure Hosts](#azure-hosts). Default: `["blob.core.windows.net"]`. |

You must provide either `connectionString`, or both `accountName` and `accountKey`. Otherwise the connector fails at startup. If `connectionString` is set, it is used.

```hocon
azure {
  connectionString: "DefaultEndpointsProtocol=https;AccountName=..."
  # or:
  accountName: "myaccount"
  accountKey: "mykey"
  maxResultsPerPage: 100
}
```

### gcp

| Parameter | Type | Required | Description |
|---|---|---|---|
| `pathToServiceKey` | String | Yes | Path to a Google Cloud service account key JSON file. The file is read when the run starts, not at construction. |
| `maxResultsPerPage` | Integer | No | Number of results returned per listing request (page size). Default: `100`. |

```hocon
gcp {
  pathToServiceKey: "/path/to/service-account.json"
  maxResultsPerPage: 100
}
```

### s3

| Parameter | Type | Required | Description |
|---|---|---|---|
| `accessKeyId` | String | No | AWS access key ID. Must be set together with `secretAccessKey`. If both are omitted, the AWS SDK's default credentials are used. |
| `secretAccessKey` | String | No | AWS secret access key. Must be set together with `accessKeyId`. |
| `region` | String | No | AWS region. If omitted, the AWS SDK resolves the region, except with `anonymous`, where it defaults to `us-east-1`. |
| `anonymous` | Boolean | No | Send unsigned requests, for public buckets that need no credentials. Cannot be combined with `accessKeyId`/`secretAccessKey`. Default: `false`. |
| `maxResultsPerPage` | Integer | No | Number of results returned per listing request (page size). Default: `1000`, which is also the most S3 returns per request. |

Setting only one of `accessKeyId`/`secretAccessKey`, or combining them with `anonymous: true`, fails at startup.

```hocon
s3 {
  accessKeyId: "AKIA..."
  secretAccessKey: "..."
  region: "us-east-1"
}
```

### Azure Hosts

An `https` URI is only read with the Azure client if its authority (host and port) contains one of `azure.allowedHosts`, which defaults to `["blob.core.windows.net"]`. This is a substring match. It applies both to `paths` and to files read by stages, such as the script for `ApplyJavascript` or the URIs fetched by `FetchFileContent`. An `https` path on any other host fails at startup, and a stage reading one fails with `No Storage Client available`. Set it to read from an Azure-compatible endpoint on another hostname. The setting only selects the Azure client: requests go to the endpoint given by the Azure credentials, so that endpoint must also be in `connectionString`. The list replaces the default, so include `blob.core.windows.net` if you still need it.

```hocon
azure {
  connectionString: "..."
  allowedHosts: ["blob.core.windows.net", "storage.example.internal"]
}
```

### Custom Storage Clients

`storageClients` maps a URI scheme to a custom `StorageClient`. Each entry is keyed by the scheme and must set `class` to the fully qualified name of a `StorageClient` implementation with a public constructor that takes a `Config`. The client receives its whole entry, including `class`, and the entry can contain any other settings the client needs. Lucille doesn't validate those settings; the client does.

Use it to add a storage system Lucille doesn't support out of the box:

```hocon
paths: ["sftp://files.example.com/data/incoming/"]
storageClients {
  sftp {
    class: "com.mycompany.lucille.storage.SftpStorageClient"
    host: "files.example.com"
    username: "ingest"
  }
}
```

Or to replace the built-in client for `s3`, `gs`, or `https`, for example with a subclass of `S3StorageClient` adapted for an S3-compatible endpoint. The entry holds both the built-in client's settings (see [s3](#s3), [gcp](#gcp), or [azure](#azure)) and any settings of the subclass's own:

```hocon
paths: ["s3://bucket/prefix/"]
storageClients {
  s3 {
    class: "com.mycompany.lucille.storage.MyS3StorageClient"
    region: "us-east-1"
    # read by MyS3StorageClient
    endpointOverride: "https://s3.storage.example.internal"
  }
}
```

Rules:

- A scheme configured in `storageClients` can't also have its built-in block: `storageClients.s3` with `s3`, `storageClients.gs` with `gcp`, or `storageClients.https` with `azure` fails at startup.
- A custom `https` client handles every `https` URI. `azure.allowedHosts` doesn't apply to it, so it must apply any host rule of its own.
- `file` can't be configured: local files are always read with the built-in client.
- Schemes must be at least two characters long, since one-letter schemes are read as Windows drive letters.

The same settings apply to stages that read files by URI, such as `FetchFileContent` and `ApplyJavascript`. See [Developing Storage Clients]({{< relref "docs/developer-guide/developing-storage-clients" >}}).

---

## Concurrent Traversal

Multiple `paths` are traversed one after another by default. Set `maxThreads` (see [Parameters](#parameters)) to traverse several at once. Here, four paths are traversed two at a time:

```hocon
paths: ["s3://bucket/a", "s3://bucket/b", "s3://bucket/c", "s3://bucket/d"]
maxThreads: 2
```

To traverse the directories of one path at once, see [Large Traversals](#large-traversals).

## Large Traversals

These settings go in the `s3`, `gcp`, or `azure` block, or in a [custom client's](#custom-storage-clients) entry in `storageClients` if that client uses the shared traversal. They don't apply to local paths.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `traversalThreads` | Integer | No | Number of threads that traverse the directories of one path. Must be at least `1`. Above `1`, `collapse` can't be used (checked at startup). Default: `1`. |
| `maxDocuments` | Long | No | The most documents to publish for each entry in `paths`. Must be at least `1`. Can't be combined with an enabled `state` block (checked at startup). No limit by default. |
| `listingMode` | String | No | `hierarchical` lists each directory separately. `flat` lists every key under the path at once, in fewer requests. `flat` can't be combined with `traversalThreads` above `1` or with `pathsToSkip` (both checked at startup). Default: `flat` on S3 unless `traversalThreads` is above `1` or `pathsToSkip` is set, and otherwise `hierarchical`. |

```hocon
paths: ["s3://bucket/a/", "s3://bucket/b/"]
maxThreads: 2
s3: {
  region: "us-east-1"
  traversalThreads: 8
}
```

**Threads.** `maxThreads` sets how many `paths` run at once, and `traversalThreads` sets how many threads work on each one, so up to `maxThreads` × `traversalThreads` threads can run. The example above can use 16. Each of them makes its own listing and content requests, so size any connection limits to suit. With an enabled `state` block, each also holds its own connection to the state database, and `moveToAfterProcessing` and `moveToErrorFolder` moves happen on these threads.

A traversal with several threads publishes the same documents as one with a single thread, but in no fixed order. With one thread, a directory's files are published first, then each of its child directories in listing order. If a thread fails, for example when a listing fails, the others stop and the run fails. Each thread uses its own instances of the configured File Handlers.

**`maxDocuments`.** Every published document counts, including each one from a File Handler or archive, so a CSV file can use up many. Once the limit is reached, listing stops and the traversal ends without failing; other entries in `paths` still get their own limit. A file whose documents were cut off by the limit isn't moved to `moveToAfterProcessing` or `moveToErrorFolder`. A limit can't be used with `state`, because files the traversal doesn't reach would look deleted and would be tombstoned and removed from state.

---

## File Handlers

File Handlers process individual files and extract one or more Lucille Documents from their contents. Without a handler, the `FileConnector` publishes one Document per file with the file metadata fields (and `file_content`, unless `getFileContent` is `false`). With a handler, a CSV file becomes one Document per row, a JSON Lines file one Document per line, and an XML file one Document per matching element. The file-level Document is not published.

File Handlers are configured under the `fileHandlers` block. Every block is validated at startup, including those for extensions that no file in the traversal has. Each key is a file extension, matched against the extension of each file without regard to case, so a `csv` handler also handles `DATA.CSV`. Two keys that differ only in case fail at startup. `csv`, `json`, `jsonl`, and `xml` use the built-in handlers unless the block sets `class`. Any other key must set `class`. If only one of `json`/`jsonl` is configured, its handler is used for both extensions. `fileHandlers`, `fileOptions`, and `filterOptions` are separate keys in the connector config block.

```hocon
fileHandlers: {
  csv {
    separatorChar: "|"
    docIdPrefix: "csv-"
  }
  json {}
  xml {
    xmlRootPath: "/records/record"
    xmlIdPath: "/records/record/id"
  }
}
```

Every File Handler accepts `class` (a custom handler class, see [Custom File Handlers](#custom-file-handlers)) and `docIdPrefix` (prepended to every Document ID it creates). The connector's own `docIdPrefix` is not applied to Documents created by a File Handler. File Handlers are built and validated against their Spec when the connector begins traversing a path, so an invalid handler config fails the run at that point rather than at startup.

### CSV File Handler

`com.kmwllc.lucille.core.filehandler.CSVFileHandler`

Extracts one Document per row from a CSV file, read as UTF-8 (a leading byte order mark is removed). The first row is the header, and each column becomes a field named after its header. Blank rows are skipped with a warning. A row whose number of columns differs from the header, or whose ID is blank as a whole, is skipped and logged as an error; if 10 consecutive rows are skipped this way, the file fails. Columns named after a reserved field (such as `id`) are not copied.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `idField` | String | No | Column whose value becomes the Document ID. Takes precedence over `idFields`. The file fails if the column is not in the header. |
| `idFields` | List\<String\> | No | Columns whose values form the Document ID, joined with `_` unless `docIdFormat` is set. The file fails if any of them is not in the header. |
| `docIdFormat` | String | No | Java `String.format` pattern applied to the `idField`/`idFields` values to build the Document ID. |
| `lineNumberField` | String | No | Field for the row number. The first row after the header is 1, and skipped rows are counted. Default: `csvLineNumber`. |
| `filenameField` | String | No | Field for the source file name. Default: `filename`. |
| `filePathField` | String | No | Field for the full file path. Default: `source`. |
| `separatorChar` | String | No | Column delimiter character. Default: `,`. |
| `useTabs` | Boolean | No | Use tab as the delimiter. Overrides `separatorChar`. Default: `false`. |
| `interpretQuotes` | Boolean | No | Treat `"` as a quoting character. Default: `true`. |
| `ignoreEscapeChar` | Boolean | No | Disable backslash escape handling. Default: `false`. |
| `lowercaseFields` | Boolean | No | Lowercase the header names used as field names. Default: `false`. |
| `ignoredTerms` | List\<String\> | No | Cell values equal to one of these strings are not added to the Document. |
| `docIdPrefix` | String | No | Prefix prepended to every Document ID. |
| `maxRecordBytes` | Integer | No | Maximum size, in bytes (UTF-8), of a single row, including every physical line of a row whose quoted value contains line breaks. Must be positive. If a row is larger, processing of the file stops with an error instead of reading the row into memory; rows before it are still published. No limit by default. |

If neither `idField` nor `idFields` is set, the Document ID is `{docIdPrefix}{filename}-{rowNumber}`. A blank value in one ID column only logs a warning, and the row is still published with the remaining ID parts (e.g. `2_`).

```hocon
fileHandlers: {
  csv {
    idField: "article_id"
    separatorChar: "|"
    filenameField: "source_file"
    lowercaseFields: true
    docIdPrefix: "article-"
  }
}
```

### JSON File Handler

`com.kmwllc.lucille.core.filehandler.JsonFileHandler`

Reads a file as JSON Lines: each non-blank line must be a single JSON object, which becomes one Document. A pretty-printed JSON object or a JSON array spanning several lines is not supported. A line that is not valid JSON, or not a JSON object, is skipped and logged as an error; if 10 consecutive lines are skipped this way, the file fails. The handler is used for both `.json` and `.jsonl` files (see above).

| Parameter | Type | Required | Description |
|---|---|---|---|
| `idField` | String | No | JSON field whose value becomes the Document ID. Takes precedence over `idFields`. |
| `idFields` | List\<String\> | No | JSON fields whose values form the Document ID, joined with `_` unless `docIdFormat` is set. |
| `docIdFormat` | String | No | Java `String.format` pattern applied to the `idField`/`idFields` values to build the Document ID. |
| `blacklist` | List\<String\> | No | JSON fields to drop from the Document. |
| `whitelist` | List\<String\> | No | Only keep these JSON fields on the Document. If both lists are set, a field is kept only if it is in `whitelist` and not in `blacklist`. |
| `docIdPrefix` | String | No | Prefix prepended to every Document ID. |
| `maxRecordBytes` | Integer | No | Maximum size, in bytes (UTF-8), of a single line. Must be positive. If a line is larger, processing of the file stops with an error instead of reading the line into memory; lines before it are still published. Default: no limit. |

Lines are read as UTF-8. If neither `idField` nor `idFields` is set, each object's own `id` field is used as the Document ID (with `docIdPrefix` prepended); a line whose `id` is missing, empty, blank, or not a string (a numeric `id` is rejected) is skipped the same way. A missing or null `idField`/`idFields` value only logs a warning and contributes an empty string to the ID; a line whose whole ID is blank is skipped. When an ID is built from `idField`/`idFields`, it overwrites any `id` field in the object. These combinations are rejected: a `blacklist` containing an ID field, a `whitelist` missing an ID field, and, without `idField`/`idFields`, a `blacklist` containing `id` or a `whitelist` without `id`.

```hocon
fileHandlers: {
  json {
    idField: "doc_id"
    blacklist: ["internal_metadata", "_rev"]
    docIdPrefix: "doc-"
  }
}
```

### XML File Handler

`com.kmwllc.lucille.core.filehandler.XMLFileHandler`

Streams an XML file and publishes one Document per element found at `xmlRootPath`. The element's raw XML text, including its own tags, is stored in `outputField`.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `xmlRootPath` | String | Yes | Slash-separated path from the document root to the elements to publish, e.g. `/Company/Staff`. Matched exactly against qualified element names (including any namespace prefix); this is not an XPath expression. |
| `xmlIdPath` | String | No | Absolute slash-separated path to the element whose text becomes the Document ID, e.g. `/Company/Staff/id`. |
| `xpathIdPath` | String | No | XPath expression, evaluated against each element, that produces the Document ID (e.g. an attribute such as `@id`). Slower than `xmlIdPath`, but more flexible. |
| `outputField` | String | No | Field for the element's XML. Default: `xml`. |
| `encoding` | String | No | Character encoding of the files. Default: `utf-8`. |
| `skipEmptyId` | Boolean | No | Skip an element whose ID is empty or blank. When `false`, such an element gets a random UUID as its ID. Default: `false`. |
| `docIdPrefix` | String | No | Prefix prepended to every Document ID. |
| `maxRecordBytes` | Integer | No | Maximum number of bytes held in memory while reading a single record. Must be positive. If exceeded, processing of the file stops with an error; records before it are still published. The limit is approximate (bytes read ahead by the parser count too) and also applies to content before the first record. No limit by default. |
| `allowDoctype` | Boolean | No | Accept files that contain a DOCTYPE. External DTDs and external entities are never loaded, even when allowed. A record that uses an entity declared in the DOCTYPE is published with a DOCTYPE declaring just those entities, so it still parses on its own; XPathExtractor then needs its own `allowDoctype`. Default: `false`, so a file with a DOCTYPE fails to parse. |

Exactly one of `xmlIdPath` and `xpathIdPath` must be set. Each Document holds only its ID and `outputField`; no source file field is added. A parse error partway through a file fails that file, but Documents already published from it are not withdrawn.

```hocon
fileHandlers: {
  xml {
    xmlRootPath: "/catalog/product"
    xmlIdPath: "/catalog/product/sku"
    outputField: "product_xml"
  }
}
```

### Video File Handler

Available via the `lucille-video` plugin (`com.kmwllc:lucille-video`) as `com.kmwllc.lucille.video.VideoFileHandler`. It uses FFmpeg through JavaCV (`javacv-platform`, which bundles the native libraries).

Extracts frames from video files, publishing one Document per sampled frame with the frame encoded as PNG bytes. It has no default extension, so configure it under each video extension with `class` set, and add the plugin jar to the classpath.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `frameStride` | Integer | No | Publish every Nth frame. Values below 1 are treated as 1. Default: `1`. |
| `docIdFormat` | String | No | `String.format` pattern with a single `%s`, applied to the file name without its extension to build the ID base. |
| `sourceField` | String | No | Field for the file path. Default: `source`. |
| `fileNameField` | String | No | Field for the file name. Default: `file_name`. |
| `frameIndexField` | String | No | Field for the zero-based frame index. Default: `frame_index`. |
| `frameTimeMsField` | String | No | Field for the frame timestamp in milliseconds. Default: `frame_time_ms`. |
| `frameTimecodeField` | String | No | Field for the timestamp as `HH:MM:SS.mmm`. Default: `frame_timecode`. |
| `imageWidthField` | String | No | Field for the frame width in pixels. Default: `image_width`. |
| `imageHeightField` | String | No | Field for the frame height in pixels. Default: `image_height`. |
| `frameImageField` | String | No | Field for the PNG bytes. Set to null if PNG encoding fails for a frame. Default: `frame_image`. |
| `docIdPrefix` | String | No | Prefix prepended to every Document ID. |

Each Document ID is `{docIdPrefix}{base}-f{frameIndex}`, where `base` is the file name without its extension (or `docIdFormat` applied to it). The frame index counts every frame, including those skipped by `frameStride`. Videos with the same file name in different folders produce the same IDs. If FFmpeg cannot open a file, that file fails; an error while reading frames stops the file after logging, keeping the frames already published.

```hocon
fileHandlers: {
  mp4 {
    class: "com.kmwllc.lucille.video.VideoFileHandler"
    frameStride: 30
  }
}
```

### Custom File Handlers

To process a file format not covered by the built-in handlers:

1. Extend `BaseFileHandler`.
2. Declare `public static final Spec SPEC = SpecBuilder.fileHandler()...` (required).
3. Provide a public constructor that takes a `Config`. It receives the handler's block from `fileHandlers`.
4. Implement `Iterator<Document> processFile(InputStream stream, String pathStr)`.

Reference the handler by its fully-qualified class name in `class`, keyed by the file extension it should handle. `class` can also override the built-in handler for `csv`, `json`, `jsonl`, or `xml`:

```hocon
fileHandlers: {
  csv {
    class: "com.example.MyCustomCSVHandler"
    myCustomParam: "value"
  }
}
```

File Handlers accept an `InputStream` and return Documents via an `Iterator`. The `InputStream` and any underlying resources are closed when the Iterator's `hasNext()` returns `false`. When working directly with File Handlers in code, always exhaust the returned Iterator. `XMLFileHandler` is the exception: its `processFile` throws, so call `processFileAndPublish` instead.

---

## File Options

File options control how the connector handles the files it finds. They go under `fileOptions`.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `getFileContent` | Boolean | No | Read each file's bytes into the `file_content` field. On cloud storage this downloads every published file and slows traversal significantly. Default: `true`. |
| `maxSizeBytes` | Long | No | Maximum size, in bytes, of file content read into `file_content`. Must be positive and at most 2147483638 (`Integer.MAX_VALUE - 9`), or the connector fails at startup. A larger file is not published and is handled as a processing error (moved to `moveToErrorFolder`, if set); its known size is checked first, so it is not downloaded. An oversized entry in an archive is skipped, and the rest of the archive is still processed. Does not apply to files processed by a File Handler; see each handler's own `maxRecordBytes`. No limit by default. |
| `handleArchivedFiles` | Boolean | No | Unpack `.zip` and `.tar` files (in any case) and process their entries. Downloads the archive on cloud storage. Default: `false`. |
| `handleCompressedFiles` | Boolean | No | Decompress `.gz` files (in any case) before processing. Combined with `handleArchivedFiles`, this also handles `.tar.gz` (but not `.tgz`). Default: `false`. |
| `moveToAfterProcessing` | String | No | Path or URI of a folder to move each file to after it is processed successfully. Only allowed with a single entry in `paths`, and must not be inside that path (checked at startup). |
| `moveToErrorFolder` | String | No | Path or URI of a folder to move a file to if an error occurs while processing it. Only allowed with a single entry in `paths`, and must not be inside that path (checked at startup). |

Setting `moveToAfterProcessing` or `moveToErrorFolder` with more than one path fails at startup, as does a folder inside the path being traversed, where moved files would be found again. For a cloud path that is a raw prefix, "inside" includes any folder whose key starts with it: `s3://bucket/incoming` rejects `s3://bucket/incoming-done/`. It also includes any folder that would put moved files back under the path: `s3://bucket/incoming/` rejects `s3://bucket/incoming`, where a file would land at `incoming/incoming/a.csv`, and `s3://bucket`, where a file would land on its own key. The folder must be on the same storage provider as the path. Archive and compressed files are moved as a whole. A moved file keeps the name its provider identifies it by:

- S3, GCS and Azure: the folder, then the object's full key or blob name. Moving `data/a.csv` to `s3://bucket/done` or `s3://bucket/done/` creates `done/data/a.csv`; moving it to `s3://other-bucket` creates `data/a.csv` in that bucket.
- Local: the file's name directly inside the folder, overwriting any file with the same name. The folder and any missing parents are created.

If processing a file fails (reading it, a File Handler error it cannot recover from, publishing, or moving it after processing), the error is logged, the file is moved to `moveToErrorFolder` if set, and traversal continues with the next file. With state, a file that fails before its processing finishes is not marked as published, so `incremental` mode retries it on the next run (a failed move after processing does not trigger a retry). A local file or directory that cannot be read during the directory walk is logged and skipped; with state, it and everything under it stay in state, so they are not tombstoned. A cloud object whose listing entry cannot be turned into a reference is logged and skipped. With state it is not marked encountered, so if an earlier run published it, it is treated as missing: once `runsBeforeExpiration` runs pass without it, it is tombstoned (if `sendTombstones` is set) and deleted from state. A local path in `paths` that does not exist or cannot be read, a local directory whose listing fails partway through, failures listing cloud storage, and a path longer than `state.pathLength` fail the run instead. A failed run publishes no tombstones and deletes nothing from state.

Entries inside archives and compressed files get composite paths using the `!` separator:

```
s3://bucket/archive.zip!path/inside/archive/file.csv
```

Filter options are applied to both the archive itself and its entries. Nested archives are not unpacked.

---

## Filter Options

Filter options control which files are processed and published. They go under `filterOptions`, and all of them are optional. A file is published only if it passes every configured check, in this order:

1. The file name must not match any `excludes` pattern, and must match at least one `includes` pattern (if any are given).
2. The file's modification time must fall within `lastModifiedCutoff` (if given).
3. In `full` publish mode (the default), the file is published.
4. In `incremental` mode, a file that was published before and has not been modified since is skipped.
5. In `incremental` mode, a file published within `lastPublishedCutoff` (if given) is skipped.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `includes` | List\<String\> | No | Regex patterns. Only files whose name matches at least one pattern are processed. |
| `excludes` | List\<String\> | No | Regex patterns. Files whose name matches any pattern are skipped. |
| `pathsToSkip` | List\<String\> | No | Directories that are not traversed at all, at any depth, including the path being traversed. See the note below. |
| `lastModifiedCutoff` | String | No | Duration (e.g., `"24h"`, `"7d"`). Only files modified within this period are processed. |
| `lastPublishedCutoff` | String | No | Duration. Files published by Lucille within this period are skipped. Requires an enabled `state` block and `publishMode: incremental`; without either, the connector fails at startup. |
| `publishMode` | String | No | `full` or `incremental` (case-insensitive). In `incremental` mode, only new or modified files are published. `incremental` requires a `state` block that is not disabled with `enabled: false`, checked at startup; any other value also fails at startup. Default: `full`. |
| `sendTombstones` | Boolean | No | Publish tombstone Documents for files that are no longer present. Requires `publishMode: incremental`, checked at startup. See [Tombstone Generation](#tombstone-generation). Default: `false`. |

Patterns must match the whole name (Java `Matcher.matches()`). For local files the name is the file name. For S3, GCS, and Azure it is the object key or blob name, which includes any folder prefix. For archive entries it is the entry name inside the archive.

Example using regex patterns and time-based filtering:

```hocon
filterOptions: {
  includes: [".*\\.pdf$", ".*\\.docx$"]
  excludes: [".*\\.tmp$", ".*~$"]
  lastModifiedCutoff: "24h"
  lastPublishedCutoff: "7d"
  publishMode: "incremental"
}
```

**Note** on `pathsToSkip`: while you could create an `excludes` regex detailing directories you do not want to publish files for, there 
are performance benefits to using `pathsToSkip` instead. With this parameter, the `FileConnector` will _not actually_ traverse the path and its contents.
If you use `excludes`, the path _will_ be traversed, but the individual documents are not processed & published.

A cloud path without a trailing `/` is a raw prefix rather than a directory, so an entry for `s3://bucket/logs/` applied to the path `s3://bucket/logs` skips the `logs/` directory and still traverses `logs-old/`.

Each entry may be a URI with a scheme (for example `s3://bucket/path`, `gs://bucket/path`, or `file:///path/to/dir`). As a convenience,
local file system paths may also be provided without a scheme - either absolute (e.g. `/path/to/dir`) or relative to the working directory
(e.g. `path/to/dir`) - and will be resolved to an absolute `file://` path.

---

## Incremental Mode and State

The `FileConnector` can persist state to a JDBC-compatible database to track which files have been published and when. State enables `incremental` publish mode, `lastPublishedCutoff`, and tombstone detection. State is used whenever a `state` block is present, unless `enabled` is `false`.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `enabled` | Boolean | No | Set to `false` to disable state without removing the block. Default: `true`. |
| `driver` | String | No | JDBC driver class. Default: `org.h2.Driver`. |
| `connectionString` | String | No | JDBC connection string. Default: `jdbc:h2:./state/{connector name}` (an embedded H2 database, whose files are written under `./state/` in the working directory). |
| `jdbcUser` | String | No | Database user. Default: `""`. |
| `jdbcPassword` | String | No | Database password. Default: `""`. |
| `tableName` | String | No | State table name. It is uppercased, and created if it does not exist. Default: the connector name. |
| `performDeletions` | Boolean | No | At the end of the run, delete rows for files that have been missing for `runsBeforeExpiration` runs. Default: `true`. |
| `pathLength` | Integer | No | Maximum length of the file path column, used only when Lucille creates the table. A longer path fails the run. Default: `768`. |
| `runsBeforeExpiration` | Integer | No | Number of consecutive runs a file must be missing before it is considered expired. Must be at least 1, checked at startup. Default: `1`. |

```hocon
state {
  enabled: true
  driver: "org.h2.Driver"
  connectionString: "jdbc:h2:./lucille-state"
  jdbcUser: ""
  jdbcPassword: ""
  tableName: "file_state"
  performDeletions: true
  pathLength: 768
  runsBeforeExpiration: 1
}
```

A few constraints to be aware of when using state:

- Files that are moved or renamed are treated as new files, since their new path is not recognised as previously published.
- Similarly, remember that the files in a directory that is skipped (by `pathsToSkip`), since they are not traversed, are also not tracked at all during a stateful run. Files that were published in an earlier run and are now under a skipped directory are therefore treated as missing: once they have been missing for `runsBeforeExpiration` runs, they expire like deleted files (tombstoned if `sendTombstones` is `true`, and removed from state if `performDeletions` is `true`).
- Capitalise directory names in `paths` consistently across runs. State lookups are case-sensitive.
- Each database table should be used by only one connector configuration. Sharing a table across connectors will corrupt state. This can happen in several ways: reusing the same explicit `tableName` across multiple connectors in the same config file; running two separate configs concurrently where both reference the same `tableName`; or omitting `tableName` in two different configs that happen to use the same connector name (since `tableName` defaults to the connector name). In all cases, two connectors writing to the same state table will overwrite each other's records, leading to incorrect incremental behavior — files may be skipped or reprocessed unexpectedly.
- The startup checks for `incremental` mode only look for a `state` block, so `state { enabled: false }` passes them. With state disabled, `incremental` mode publishes every file, `lastPublishedCutoff` is ignored without a warning, and no tombstones are sent.
- A file's publish time is recorded as the time the run started.
- Only one run at a time (within a JVM) can use a given `connectionString` **and** `tableName` for state. A run fails at startup if another run is already using them.

---

## Tombstone Generation

When `filterOptions.sendTombstones: true` is set (requires incremental mode and state), the connector detects files that existed in previous runs but were not encountered in this one. For each such file it publishes a tombstone Document: its ID is built from the file path the same way as a regular file Document, and it has `file_expired: true` and is marked skipped (`___skipped: true`), so it bypasses all pipeline stages. Downstream, configure the Indexer to issue a delete when it encounters the marker field:

```hocon
indexer {
  type: "solr"
  deletionMarkerField: "file_expired"
  deletionMarkerFieldValue: "true"
}
```

Tombstone IDs match the IDs of per-file Documents, including archive entries and decompressed files. Documents created by a File Handler have their own IDs, so tombstones do not match them. A tombstone also carries `file_path`, `file_size_bytes` of `0`, and the current time as `file_modification_date` and `file_creation_date`. The archive or compressed file itself is tracked in state too, so deleting one also produces a tombstone for the container, which matches no indexed Document.

Tombstones are published at the end of the traversal, and expired rows are deleted from state when the connector closes if `performDeletions` is `true`. If the tombstones cannot all be published, the run fails and the expired rows are kept, so their tombstones are published on the next run. With `performDeletions: false`, expired rows stay in the table, so their tombstones are published again on every later run.

By default, a file is considered expired after being absent from a single run. You can increase this threshold with `state.runsBeforeExpiration` to provide a safety margin against transient storage issues (e.g., a temporary listing failure):

```hocon
state {
  runsBeforeExpiration: 3   # File must be absent for 3 consecutive runs before tombstone
}
```

The value must be at least 1. When set to 1 (default), a file missing from one run immediately triggers a tombstone.

---

## Document Fields

Each per-file Document published by the `FileConnector` has an ID of `{docIdPrefix}` plus the MD5 hash of its `file_path`, and these fields:

| Field | Type | Description |
|---|---|---|
| `file_path` | String | URI of the file (e.g. `file:///C:/data/a.json`, `s3://bucket/a.json`). Always uses `/` as the separator. |
| `file_modification_date` | Instant | Last-modified timestamp of the file, where the storage provider reports one. |
| `file_creation_date` | Instant | Creation timestamp of the file, where the storage provider reports one. Never set for S3 objects or archive entries. |
| `file_size_bytes` | Long | File size in bytes. May be `-1` for some archive entries. For a decompressed `.gz` file, the size of the compressed file. |
| `file_content` | byte[] | Raw file bytes. Set unless `getFileContent` is `false`. |
| `file_expired` | Boolean | Set to `true` on tombstone Documents for deleted files. |

When a File Handler (CSV, JSON, XML) processes a file, it publishes its own Documents with the fields described in its section above, instead of a per-file Document. None of the fields in this table are added to them; only the CSV and video handlers record the source file path.

These field names are part of the StorageClient contract, and are available as constants on `com.kmwllc.lucille.connector.storageclient.StorageClientFields`. See [Developing Storage Clients]({{< relref "docs/developer-guide/developing-storage-clients#fields-your-client-emits" >}}).

---

## Implementation Notes

*This section is for Component Developers implementing new connectors. Pipeline Authors can stop here.*

### StorageClient Pattern

The connector handles four storage backends through a `StorageClient` abstraction. During traversal, the appropriate client is selected by URI scheme (`file`, `s3`, `gs`, or `https` for Azure):

```java
private void traverseStoragePath(Publisher publisher, URI pathToTraverse) throws ConnectorException {
    StorageClient storageClient = storageClientMap.get(clientKeyFor(pathToTraverse));
    // ...
    storageClient.traverse(publisher, params, stateManager);
}
```

`StorageClient` is an interface with implementations for each backend. Each implementation handles listing/paginating files, applying filter criteria, reading file content, publishing documents, and updating state. The `StorageClient.createClients(config)` factory method always creates a local client, inspects the config for cloud provider blocks and `storageClients` entries, and instantiates the matching clients.

### Lifecycle: execute → close

Storage clients and the state manager are constructed in the constructor, but they connect inside `execute()`. This avoids opening network connections before the connector actually runs. Abridged:

```java
@Override
public void execute(Publisher publisher) throws ConnectorException {
    initialize();  // init storage clients + state manager

    if (concurrent && storageURIs.size() > 1) {
        traversePathsConcurrently(publisher);
    } else {
        for (URI resource : storageURIs) {
            traverseStoragePath(publisher, resource);
        }
    }

    // ... update state counters, then send tombstones if filterOptions.sendTombstones is true
}

@Override
public void close() {
    if (stateManager != null) stateManager.shutdown();
    for (StorageClient client : storageClientMap.values()) {
        client.stop();
    }
}
```

### SPEC Declaration

The `FileConnector` SPEC demonstrates how to declare a complex, nested configuration:

```java
public static final Spec SPEC = SpecBuilder.connector()
    .requiredList("paths", new TypeReference<List<String>>(){})
    .optionalNumber("maxThreads")
    .optionalParent("filterOptions", SpecBuilder.withoutDefaults()
        .optionalList("includes", new TypeReference<List<String>>(){})
        .optionalList("excludes", new TypeReference<List<String>>(){})
        .optionalList("pathsToSkip", new TypeReference<List<String>>(){})
        .optionalString("lastModifiedCutoff", "lastPublishedCutoff", "publishMode")
        .optionalBoolean("sendTombstones").build())
    .optionalParent("fileOptions", SpecBuilder.withoutDefaults()
        .optionalBoolean("getFileContent", "handleArchivedFiles", "handleCompressedFiles")
        .optionalNumber("maxSizeBytes")
        .optionalString("moveToAfterProcessing", "moveToErrorFolder").build())
    .optionalParent("state", SpecBuilder.withoutDefaults()
        .optionalString("driver", "connectionString", "jdbcUser", "jdbcPassword", "tableName")
        .optionalBoolean("performDeletions", "enabled")
        .optionalNumber("pathLength", "runsBeforeExpiration").build())
    .include(STORAGE_SPEC)
    .optionalParent("fileHandlers", new TypeReference<Map<String, Map<String, Object>>>(){}).build();
```

Key patterns: `SpecBuilder.connector()` provides base properties; `optionalParent(key, spec)` defines each optional config section; the cloud provider blocks and `storageClients` come from `STORAGE_SPEC`, which the stages that read files include too; `fileHandlers` uses a `TypeReference` because its structure is dynamic (keys are file extensions).

### Reusable Patterns for Other Complex Connectors

1. **StorageClient pattern** — abstract the data source behind an interface; select implementation by URI scheme or config key.
2. **Deferred initialisation** — don't open connections in the constructor; do it in `execute()`.
3. **Constraint validation early** — check for incompatible config combinations (e.g., multiple paths + `moveToAfterProcessing`) in the constructor so failures surface before any traversal begins.
4. **Nested SPEC declarations** — group related config into parent blocks; share a group of parents across components by putting them in one Spec and adding it with `include()`.
5. **JDBC-backed state** — make state optional; check `config.hasPath("state")` before constructing the state manager.
6. **Filter pipeline** — layer multiple filter criteria (regex, time-based, state-based) with a clear evaluation order.
7. **Tombstone generation** — detect deletions by comparing the current file set to the previous state snapshot; mark documents with a field the Indexer can act on.
8. **FileHandler delegation** — use pluggable handlers (with their own SPECs) for format-specific processing rather than embedding format logic in the connector.
