---
title: File Connector
weight: 1
date: 2025-02-28
description: A Connector that traverses local filesystems and cloud storage (S3, Azure, GCS), applies pluggable file handlers, and publishes Lucille documents. Supports incremental mode, tombstone deletions, and archive unpacking.
aliases:
  - /docs/reference/connectors/file_handlers/
---

[Source Code](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/FileConnector.java)

The `FileConnector` traverses a file system and publishes a Lucille Document for each file it encounters. It supports local filesystems, Amazon S3, Azure Blob Storage, and Google Cloud Storage through a unified interface — a single connector config can traverse paths across multiple providers simultaneously. Optional File Handlers extract structured Documents from files that themselves contain data (CSV rows, JSON objects, XML elements).

---

## Cloud Storage Configuration

When traversing cloud storage, provide authentication under the appropriate top-level config block alongside your connector config. Each provider also accepts an optional `maxNumOfPages` to limit how many file listings are loaded into memory per request.

**Azure**

```hocon
azure {
  connectionString: "DefaultEndpointsProtocol=https;AccountName=..."
  # or:
  accountName: "myaccount"
  accountKey: "mykey"
  maxNumOfPages: 100
}
```

You must provide either `connectionString`, or both `accountName` and `accountKey`.

**Google Cloud Storage**

```hocon
gcp {
  pathToServiceKey: "/path/to/service-account.json"
  maxNumOfPages: 100
}
```

**Amazon S3**

```hocon
s3 {
  accessKeyId: "AKIA..."
  secretAccessKey: "..."
  region: "us-east-1"
  maxNumOfPages: 100
}
```

For S3 paths, percent-encode special characters in `paths` (e.g., `s3://bucket/folder%20with%20spaces`).

A single connector can traverse multiple paths across providers:

```hocon
paths: ["file:///local/data", "s3://my-bucket/prefix", "az://container/path"]
```

The URI scheme selects the appropriate storage backend automatically. A local filesystem client is always available without additional configuration.

---

## File Handlers

File Handlers process individual files and extract one or more Lucille Documents from their contents. Without a handler, the `FileConnector` publishes one document per file containing only the file metadata fields. With a handler, a CSV file becomes one document per row, a JSON file becomes one document per object, and so on.

File Handlers are configured under the `fileHandlers` block. The key (`csv`, `json`, `xml`) determines which handler applies to files with that extension. `fileHandlers` and `fileOptions` are two separate top-level keys in the connector config block — they are not nested inside each other.

```hocon
fileHandlers: {
  csv {
    separatorChar: "|"
    docIdPrefix: "csv-"
  }
  json {}
  xml {
    chunkPath: "//record"
  }
}
```

All File Handlers support `docIdPrefix` (prepended to every generated Document ID).

### CSV File Handler

`com.kmwllc.lucille.core.fileHandler.CSVFileHandler`

Extracts one Document per row from a CSV file.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `idField` | String | — | Single column name whose value becomes the Document ID. |
| `idFields` | List\<String\> | — | Multiple column names combined to form the Document ID. |
| `docIdFormat` | String | — | Java `String.format` pattern for constructing the Document ID from column values. |
| `lineNumberField` | String | `csvLineNumber` | Field name for storing the row's line number. |
| `filenameField` | String | `filename` | Field name for storing the source filename. |
| `filePathField` | String | `source` | Field name for storing the full file path. |
| `separatorChar` | String | `,` | Column delimiter character. |
| `useTabs` | Boolean | `false` | Use tab as delimiter (overrides `separatorChar`). |
| `interpretQuotes` | Boolean | `true` | Treat `"` as a quoting character. |
| `ignoreEscapeChar` | Boolean | `false` | Disable backslash escape handling. |
| `lowercaseFields` | Boolean | `false` | Convert column header names to lowercase field names. |
| `ignoredTerms` | List\<String\> | — | Column values matching these strings are excluded from the document. |
| `docIdPrefix` | String | — | Prefix prepended to every Document ID. |

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

`com.kmwllc.lucille.core.fileHandler.JSONFileHandler`

Extracts one Document per JSON object. Supports both standard JSON files (a single object or array) and JSON Lines (`.jsonl`) format where each line is a separate JSON object.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `idField` | String | — | JSON field whose value becomes the Document ID. |
| `idFields` | List\<String\> | — | Multiple JSON fields combined to form the Document ID. |
| `docIdFormat` | String | — | Java `String.format` pattern for constructing the Document ID. |
| `blacklist` | List\<String\> | — | JSON fields to exclude from the Document. |
| `whitelist` | List\<String\> | — | Only include these JSON fields on the Document. |
| `docIdPrefix` | String | — | Prefix prepended to every Document ID. |

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

`com.kmwllc.lucille.core.fileHandler.XMLFileHandler`

Extracts Documents from an XML file by selecting elements matching an XPath expression. Each matched element becomes a Document; element child text content and attributes become Document fields.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `chunkPath` | String | Yes | XPath expression selecting the XML elements to convert into Documents (e.g., `"//record"`, `"/root/items/item"`). |
| `docIdPrefix` | String | No | Prefix prepended to every Document ID. |

```hocon
fileHandlers: {
  xml {
    chunkPath: "//product"
  }
}
```

### Video File Handler

Available via the `lucille-video` plugin (`com.kmwllc:lucille-video`). Requires FFmpeg to be installed on the system.

Extracts frames from video files as individual Lucille Documents. See the plugin documentation for configuration parameters and supported formats.

### Custom File Handlers

To process a file format not covered by the built-in handlers:

1. Extend `BaseFileHandler`.
2. Declare `public static final Spec SPEC = SpecBuilder.fileHandler()...` (required).
3. Implement `Iterator<Document> processFile(InputStream stream, String pathStr)`.

Reference the handler by its fully-qualified class name in the `fileHandlers` block. The `class` field can also override the built-in handler for a standard extension:

```hocon
fileHandlers: {
  csv {
    class: "com.example.MyCustomCSVHandler"
    myCustomParam: "value"
  }
}
```

File Handlers accept an `InputStream` and return Documents via an `Iterator`. The `InputStream` and any underlying resources are closed when the Iterator's `hasNext()` returns `false`. When working directly with File Handlers in code, always exhaust the returned Iterator.

---

## File Options

File options control traversal behaviour — how the connector handles the files it finds.

| Option | Type | Description |
|---|---|---|
| `getFileContent` | Boolean | If `true`, reads the file's raw bytes into the `file_content` field. **Downloads the file on cloud storage. Slows traversal significantly.** |
| `handleArchivedFiles` | Boolean | If `true`, unpacks archive files (zip, tar, tar.gz) and traverses their contents. Downloads the archive on cloud storage. |
| `handleCompressedFiles` | Boolean | If `true`, decompresses compressed files (gz) before processing. |
| `moveToAfterProcessing` | String | Path to move each file to after successful processing. Single-path configurations only — cannot be combined with multiple `paths`. |
| `moveToErrorFolder` | String | Path to move a file to if an error occurs during processing. Same single-path constraint applies. |

When archive or compressed file handling is enabled, entries inside archives get composite paths using the `!` separator:

```
s3://bucket/archive.zip!path/inside/archive/file.csv
```

Modification and publish cutoffs apply to both the container archive and its entries.

---

## Filter Options

Filter options control which files are processed and published. All filter options are optional. When multiple options are specified, a file must satisfy all of them to be processed. Evaluation order:

1. File name must match at least one `includes` pattern (if any are specified).
2. File name must not match any `excludes` pattern.
3. File's modification time must fall within `lastModifiedCutoff` (if specified).
4. File must not have been published within `lastPublishedCutoff` (if specified — requires state configuration).
5. In `incremental` publish mode, only new or modified files since the last run are published (requires state configuration).

| Option | Type | Description |
|---|---|---|
| `includes` | List\<String\> | Regex patterns — only file names matching at least one pattern are processed. |
| `excludes` | List\<String\> | Regex patterns — file names matching any pattern are skipped. |
| `lastModifiedCutoff` | String | Duration string (e.g., `"24h"`, `"7d"`) — only files modified within this window are processed. |
| `lastPublishedCutoff` | String | Duration string — files published by Lucille within this window are skipped. Requires state. |
| `publishMode` | String | `FULL` (default) or `INCREMENTAL`. In incremental mode, only new or modified files are published. Requires state. |
| `sendTombstones` | Boolean | If `true`, publishes tombstone documents for files that have been deleted since the last run. Requires incremental mode. |

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

---

## Incremental Mode and State

The `FileConnector` can persist state to a JDBC-compatible database to track which files have been published and when. State enables `lastPublishedCutoff`, incremental publish mode, and tombstone detection.

Configure a state database alongside the connector:

```hocon
state {
  driver: "org.h2.Driver"          # Default: embedded H2
  connectionString: "jdbc:h2:./lucille-state"
  jdbcUser: ""
  jdbcPassword: ""
  tableName: "file_state"          # Defaults to the connector name
  performDeletions: true
  pathLength: 200                  # Max length of the file path column
}
```

If `connectionString` is omitted, an embedded H2 database is created at `./state/{CONNECTOR_NAME}`.

A few constraints to be aware of when using state:

- Files that are moved or renamed will not have `lastPublishedCutoff` applied — their new path is not recognised as previously published.
- Capitalise directory names in `paths` consistently across runs. State lookups are case-sensitive.
- Each database table should be used for only one connector configuration. Sharing a table across connectors will corrupt state.

---

## Tombstone Generation

When `filterOptions.sendTombstones: true` is set (requires incremental mode and state), the connector detects files that existed in the previous run but are no longer present. For each deleted file it publishes a tombstone document — a document with `file_expired: true` and `___skipped: true`. The skipped flag causes the tombstone to bypass all pipeline stages. Downstream, configure the Indexer to issue a delete when it encounters the marker field:

```hocon
indexer {
  type: "solr"
  deletionMarkerField: "file_expired"
  deletionMarkerFieldValue: "true"
}
```

This parameter only applies in incremental mode.

---

## Document Fields

Every document published by the `FileConnector` carries these fields:

| Field | Type | Description |
|---|---|---|
| `file_path` | String | Full path or URI to the file. |
| `file_modification_date` | Instant | Last-modified timestamp of the file. |
| `file_creation_date` | Instant | Creation timestamp of the file (where available). |
| `file_size_bytes` | Long | File size in bytes. |
| `file_content` | byte[] | Raw file bytes. Only populated when `getFileContent: true`. |
| `file_expired` | Boolean | Set to `true` on tombstone documents for deleted files. |

When a File Handler (CSV, JSON, XML) processes a file, it produces child documents with their own fields rather than a single file-level document.

---

## Implementation Notes

*This section is for Component Developers implementing new connectors. Pipeline Authors can stop here.*

### StorageClient Pattern

The connector handles four storage backends through a `StorageClient` abstraction. During traversal, the appropriate client is selected by URI scheme:

```java
private void traverseStoragePath(Publisher publisher, URI pathToTraverse) throws ConnectorException {
    String clientKey = pathToTraverse.getScheme() != null ? pathToTraverse.getScheme() : "file";
    StorageClient storageClient = storageClientMap.get(clientKey);
    // ...
    storageClient.traverse(publisher, params, stateManager);
}
```

`StorageClient` is an interface with implementations for each backend. Each implementation handles listing/paginating files, applying filter criteria, reading file content, publishing documents, and updating state. The `createClients(config)` factory method inspects the config for cloud provider blocks and instantiates the appropriate clients.

### Lifecycle: execute → close

Initialisation of storage clients and the state manager is deferred to inside `execute()`, not the constructor. This avoids opening network connections before the connector actually runs:

```java
@Override
public void execute(Publisher publisher) throws ConnectorException {
    initialize();  // Init storage clients + state manager

    for (URI resource : storageURIs) {
        traverseStoragePath(publisher, resource);
    }

    if (sendTombstones) {
        sendExpiredFileTombstones(publisher);
    }
}

@Override
public void close() {
    if (stateManager != null) stateManager.shutdown();
    for (StorageClient client : storageClientMap.values()) {
        client.shutdown();
    }
}
```

### SPEC Declaration

The `FileConnector` SPEC demonstrates how to declare a complex, nested configuration:

```java
public static final Spec SPEC = SpecBuilder.connector()
    .requiredList("paths", new TypeReference<List<String>>(){})
    .optionalParent(
        SpecBuilder.parent("filterOptions")
            .optionalList("includes", new TypeReference<List<String>>(){})
            .optionalList("excludes", new TypeReference<List<String>>(){})
            .optionalString("lastModifiedCutoff", "lastPublishedCutoff", "publishMode", "sendTombstones").build(),
        SpecBuilder.parent("fileOptions")
            .optionalBoolean("getFileContent", "handleArchivedFiles", "handleCompressedFiles")
            .optionalString("moveToAfterProcessing", "moveToErrorFolder").build(),
        SpecBuilder.parent("state")
            .optionalString("driver", "connectionString", "jdbcUser", "jdbcPassword", "tableName")
            .optionalBoolean("performDeletions")
            .optionalNumber("pathLength").build(),
        GCP_PARENT_SPEC,
        AZURE_PARENT_SPEC,
        S3_PARENT_SPEC)
    .optionalParent("fileHandlers", new TypeReference<Map<String, Map<String, Object>>>(){})
    .build();
```

Key patterns: `SpecBuilder.connector()` provides base properties; nested `SpecBuilder.parent(...)` blocks define each optional config section; cloud provider specs are defined as reusable constants; `fileHandlers` uses a `TypeReference` because its structure is dynamic (keys are file extensions).

### Reusable Patterns for Other Complex Connectors

1. **StorageClient pattern** — abstract the data source behind an interface; select implementation by URI scheme or config key.
2. **Deferred initialisation** — don't open connections in the constructor; do it in `execute()`.
3. **Constraint validation early** — check for incompatible config combinations (e.g., multiple paths + `moveToAfterProcessing`) in the constructor so failures surface before any traversal begins.
4. **Nested SPEC declarations** — group related config into `parent` blocks; define cloud provider specs as reusable constants shared across connectors.
5. **JDBC-backed state** — make state optional; check `config.hasPath("state")` before constructing the state manager.
6. **Filter pipeline** — layer multiple filter criteria (regex, time-based, state-based) with a clear evaluation order.
7. **Tombstone generation** — detect deletions by comparing the current file set to the previous state snapshot; mark documents with a field the Indexer can act on.
8. **FileHandler delegation** — use pluggable handlers (with their own SPECs) for format-specific processing rather than embedding format logic in the connector.
