---
title: "FileConnector Case Study"
weight: 60
date: 2025-06-09
description: >
  A deep dive into the FileConnector as a case study for patterns reusable in complex connectors.
---

## Overview

The `FileConnector` traverses local and cloud storage systems, discovers files, and publishes a Document for each one. It's one of Lucille's most complex connectors, supporting local filesystems, Amazon S3, Azure Blob Storage, and Google Cloud Storage through a unified interface. It serves as an excellent case study for patterns reusable in other complex connectors.

## Multi-Cloud Support via StorageClient

The same connector handles four storage backends through the `StorageClient` abstraction:

```java
private final Map<String, StorageClient> storageClientMap;

// Created from config — one client per URI scheme
this.storageClientMap = StorageClient.createClients(config);
```

During traversal, the appropriate client is selected by URI scheme:

```java
private void traverseStoragePath(Publisher publisher, URI pathToTraverse) throws ConnectorException {
    String clientKey = pathToTraverse.getScheme() != null ? pathToTraverse.getScheme() : "file";
    StorageClient storageClient = storageClientMap.get(clientKey);

    if (storageClient == null) {
        throw new ConnectorException("No StorageClient was available for (" + pathToTraverse +
            "). Did you include the necessary configuration?");
    }

    TraversalParams params = buildTraversalParams(pathToTraverse);
    storageClient.traverse(publisher, params, stateManager);
}
```

This means a single connector config can traverse multiple paths across different providers:

```hocon
paths = ["file:///local/data", "s3://my-bucket/prefix", "az://container/path"]
```

## The StorageClient Pattern

`StorageClient` is an interface with implementations for each backend. The key method:

```java
void traverse(Publisher publisher, TraversalParams params, FileConnectorStateManager stateManager)
```

Each implementation handles:
- Listing/paginating files in its native API
- Applying filter criteria (includes, excludes, modification time)
- Reading file content (if configured)
- Publishing documents via the Publisher
- Updating state (if state manager is provided)

The `createClients(config)` factory method inspects the config for cloud provider blocks (`s3`, `azure`, `gcp`) and instantiates the appropriate clients. A local filesystem client is always available.

## Incremental State Tracking

The `FileConnectorStateManager` provides JDBC-backed state persistence:

```java
this.stateManager = config.hasPath("state") ? 
    new FileConnectorStateManager(config.getConfig("state"), getName()) : null;
```

State tracks:
- **File paths** that have been published
- **Last publish timestamps** for each file

This enables:
- **`lastPublishedCutoff`** — skip files published within a recent time window
- **Incremental mode** — only publish files that are new or modified since last run
- **Tombstone detection** — identify files that existed in previous runs but are now gone

Configuration:
```hocon
state {
    driver = "org.h2.Driver"                          # Default
    connectionString = "jdbc:h2:./state/myConnector"  # Default: embedded H2
    jdbcUser = ""
    jdbcPassword = ""
    tableName = "myConnector"                         # Defaults to connector name
    performDeletions = true
    pathLength = 200                                  # Max path column length
}
```

If `connectionString` is omitted, an embedded H2 database is created at `./state/{CONNECTOR_NAME}`.

## File Filtering

The `filterOptions` block controls which files are processed:

```hocon
filterOptions {
    includes = [".*\\.pdf$", ".*\\.docx$"]    # Regex whitelist
    excludes = [".*\\.tmp$", ".*~$"]          # Regex blacklist
    lastModifiedCutoff = "24h"                 # Only files modified in last 24 hours
    lastPublishedCutoff = "7d"                 # Skip files published in last 7 days
    publishMode = "incremental"                # "full" or "incremental"
}
```

Filtering logic:
1. File must match at least one `includes` pattern (if specified)
2. File must NOT match any `excludes` pattern
3. File's modification time must be within `lastModifiedCutoff` (if specified)
4. File must not have been published within `lastPublishedCutoff` (requires state)
5. In `incremental` mode, only new/modified files since last run are published

**Important constraint**: `publishMode = "incremental"` requires state configuration:
```java
if (mode == PublishMode.INCREMENTAL && !config.hasPath("state")) {
    throw new IllegalArgumentException(
        "filterOptions.publishMode of 'incremental' requires state configuration.");
}
```

## FileHandlers: Pluggable Format Processing

Different file formats can be processed by pluggable handlers:

```hocon
fileHandlers {
    csv {
        class = "com.kmwllc.lucille.connector.CsvFileHandler"
        docIdPrefix = "csv-"
    }
    json {
        class = "com.kmwllc.lucille.connector.JsonFileHandler"
    }
}
```

FileHandlers have their own SPEC (via `SpecBuilder.fileHandler()`) with default legal properties `class` and `docIdPrefix`. They receive file content and produce one or more Documents from it.

## Tombstone Generation

When `filterOptions.sendTombstones = true` (requires incremental mode), the connector detects deleted files:

```java
private void sendExpiredFileTombstones(Publisher publisher) throws ConnectorException {
    if (stateManager == null) return;

    List<URI> expiredFileUris = stateManager.listExpiredFiles();
    
    for (URI uri : expiredFileUris) {
        Document doc = buildTombstoneDoc(uri);
        publisher.publish(doc);
    }
}
```

A tombstone document is marked with `file_expired = true` and `skipped = true`:

```java
private Document buildTombstoneDoc(URI uri) {
    Document doc = BaseFileReference.buildBaseDoc(uri.toString(), now, 0L, now, params);
    doc.setField(EXPIRED, true);
    doc.setSkipped(true);
    return doc;
}
```

The `skipped` flag causes the document to bypass pipeline stages. Downstream, the Indexer can be configured to delete documents marked with a specific field/value combination (`indexer.deletionMarkerField` / `indexer.deletionMarkerFieldValue`).

## Archive Handling

When `fileOptions.handleArchivedFiles = true` or `fileOptions.handleCompressedFiles = true`, the StorageClient traverses into archive files (zip, tar, etc.):

```hocon
fileOptions {
    getFileContent = true
    handleArchivedFiles = true
    handleCompressedFiles = true
}
```

Archive entries get composite paths using the `ARCHIVE_FILE_SEPARATOR` (`!`):
```
s3://bucket/archive.zip!path/inside/archive/file.txt
```

Modification/publish cutoffs apply to both the container archive and its entries.

## File Movement After Processing

For single-path configurations, files can be moved after processing:

```hocon
fileOptions {
    moveToAfterProcessing = "s3://bucket/processed/"
    moveToErrorFolder = "s3://bucket/errors/"
}
```

**Constraint**: Cannot be used with multiple paths:
```java
if (storageURIs.size() > 1 && (config.hasPath("fileOptions.moveToAfterProcessing") || 
    config.hasPath("fileOptions.moveToErrorFolder"))) {
    throw new IllegalArgumentException(
        "FileConnector does not support multiple paths and moveToAfterProcessing / moveToErrorFolder.");
}
```

## The SPEC Declaration

The FileConnector's SPEC demonstrates how a complex connector declares its configuration:

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

Key patterns:
- `SpecBuilder.connector()` provides base properties (name, class, pipeline, docIdPrefix, collapse)
- Nested `SpecBuilder.parent(...)` blocks define each optional config section
- Cloud provider specs are defined as reusable constants
- `fileHandlers` uses a `TypeReference` because its structure is dynamic (keys are file extensions)

## Lifecycle: preExecute → execute → close

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

The `initialize()` method is called inside `execute()` (not in the constructor) because it may involve network connections that should only be established when the connector actually runs.

## Document Fields Published

Each file produces a document with standard fields:

| Field | Constant | Content |
|-------|----------|---------|
| `file_path` | `FILE_PATH` | Full URI of the file |
| `file_modification_date` | `MODIFIED` | Last modification timestamp |
| `file_creation_date` | `CREATED` | Creation timestamp |
| `file_size_bytes` | `SIZE` | File size |
| `file_content` | `CONTENT` | Raw file content (if `getFileContent = true`) |
| `file_expired` | `EXPIRED` | `true` for tombstone documents |

## Reusable Patterns for Other Complex Connectors

1. **StorageClient pattern** — abstract the data source behind an interface; select implementation by URI scheme or config
2. **State management** — use JDBC-backed state for incremental processing; make it optional
3. **Nested SPEC declarations** — group related config into parent blocks; define cloud specs as reusable constants
4. **Filter pipeline** — layer multiple filter criteria (regex, time-based, state-based)
5. **Tombstone generation** — detect deletions by comparing current state to previous state
6. **Deferred initialization** — don't open connections in the constructor; do it in `execute()`
7. **Constraint validation in constructor** — check for incompatible config combinations early
8. **FileHandler delegation** — use pluggable handlers for format-specific processing
