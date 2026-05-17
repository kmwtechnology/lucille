---
title: File Ingestion Cookbook
weight: 1
date: 2025-06-09
description: Recipes for ingesting files from the local filesystem, Amazon S3, Azure Blob Storage, and Google Cloud Storage.
---

This cookbook covers common file ingestion patterns using Lucille's `FileConnector`.

## Recipe 1: Ingest CSV Files from Local Filesystem

Read all `.csv` files in a directory and index each row as a document into OpenSearch.

```hocon
connectors: [
  {
    name: "csv-connector"
    class: "com.kmwllc.lucille.connector.FileConnector"
    pipeline: "csv-pipeline"
    paths: ["/data/csvfiles"]

    fileHandlers: {
      csv {
        filenameField: "source_file"
        docIdPrefix: "row-"
      }
    }

    filterOptions: {
      includes: [".*\\.csv$"]
    }
  }
]

pipelines: [
  {
    name: "csv-pipeline"
    stages: [
      {
        class: "com.kmwllc.lucille.stage.TrimWhitespace"
        fields: ["title", "description"]
      }
    ]
  }
]

indexer { type: "opensearch" }

opensearch {
  url: "https://localhost:9200"
  index: "my-docs"
  acceptInvalidCert: true
}
```

## Recipe 2: Ingest JSON Files

Read `.json` or `.jsonl` files. Each top-level JSON object (or each line in a `.jsonl` file) becomes a Document.

```hocon
connectors: [
  {
    name: "json-connector"
    class: "com.kmwllc.lucille.connector.FileConnector"
    pipeline: "json-pipeline"
    paths: ["/data/jsonfiles"]

    fileHandlers: {
      json {}
    }
  }
]
```

For `.jsonl` files (one JSON object per line), no extra configuration is needed — the `JSONFileHandler` handles both formats automatically.

## Recipe 3: Ingest XML Files

Extract records from XML files using an XPath expression.

```hocon
connectors: [
  {
    name: "xml-connector"
    class: "com.kmwllc.lucille.connector.FileConnector"
    pipeline: "xml-pipeline"
    paths: ["/data/xmlfiles"]

    fileHandlers: {
      xml {
        chunkPath: "//record"
      }
    }
  }
]
```

Each XML element matching `//record` becomes a separate Lucille Document.

## Recipe 4: Ingest from Amazon S3

```hocon
connectors: [
  {
    name: "s3-connector"
    class: "com.kmwllc.lucille.connector.FileConnector"
    pipeline: "my-pipeline"
    paths: ["s3://my-bucket/data/"]

    s3 {
      accessKeyId: ${?AWS_ACCESS_KEY_ID}
      secretAccessKey: ${?AWS_SECRET_ACCESS_KEY}
      region: ${?AWS_DEFAULT_REGION}
    }

    fileHandlers: {
      csv {}
    }
  }
]
```

For paths with special characters (e.g., spaces), percent-encode the URI: `s3://my-bucket/folder%20with%20spaces/`.

## Recipe 5: Ingest from Azure Blob Storage

```hocon
connectors: [
  {
    name: "azure-connector"
    class: "com.kmwllc.lucille.connector.FileConnector"
    pipeline: "my-pipeline"
    paths: ["https://mystorageaccount.blob.core.windows.net/my-container/"]

    azure {
      connectionString: ${?AZURE_CONNECTION_STRING}
    }
  }
]
```

Alternatively, authenticate with account name and key:

```hocon
azure {
  accountName: ${?AZURE_ACCOUNT_NAME}
  accountKey: ${?AZURE_ACCOUNT_KEY}
}
```

## Recipe 6: Ingest from Google Cloud Storage

```hocon
connectors: [
  {
    name: "gcs-connector"
    class: "com.kmwllc.lucille.connector.FileConnector"
    pipeline: "my-pipeline"
    paths: ["gs://my-bucket/data/"]

    gcp {
      pathToServiceKey: ${?GCP_SERVICE_KEY_PATH}
    }
  }
]
```

## Recipe 7: Incremental Ingest (Only New or Modified Files)

Use state tracking to skip files that have not changed since the last run.

```hocon
connectors: [
  {
    name: "incremental-connector"
    class: "com.kmwllc.lucille.connector.FileConnector"
    pipeline: "my-pipeline"
    paths: ["/data/files"]
    publishMode: "INCREMENTAL"

    # JDBC state database (H2 embedded is the default; Derby also works)
    state {
      driver: "org.h2.Driver"
      connectionString: "jdbc:h2:./lucille-state"
    }

    filterOptions: {
      # Only process files that were last published more than 1 hour ago
      lastPublishedCutoff: "1h"
    }
  }
]
```

**Notes on incremental mode:**
- `FULL` mode (the default) republishes every file on every run.
- `INCREMENTAL` mode requires a `state` database and only processes files that are new or modified since the last run.
- The `state` database tracks when each file was last published by Lucille.

## Recipe 8: Tombstone Deletions

Automatically detect deleted files and issue deletes against the search backend.

```hocon
connectors: [
  {
    name: "connector-with-tombstones"
    class: "com.kmwllc.lucille.connector.FileConnector"
    pipeline: "my-pipeline"
    paths: ["/data/files"]
    publishMode: "INCREMENTAL"
    sendTombstones: true

    state {
      driver: "org.h2.Driver"
      connectionString: "jdbc:h2:./lucille-state"
    }
  }
]

indexer {
  type: "opensearch"
  deletionMarkerField: "file_expired"
  deletionMarkerFieldValue: "true"
}
```

When a file is deleted from the filesystem, Lucille creates a tombstone Document with `file_expired: true`. The `OpenSearchIndexer` sees this marker and issues a delete request against the search backend.

## Recipe 9: Extract Text from PDF, Office, and Other Formats (Tika)

Use the `lucille-tika` plugin to extract text from arbitrary file formats. Set `getFileContent: true` in `fileOptions` to read raw bytes into `file_content`, then pass it to the `TextExtractor` stage.

```hocon
connectors: [
  {
    name: "docs-connector"
    class: "com.kmwllc.lucille.connector.FileConnector"
    pipeline: "docs-pipeline"
    paths: ["/data/documents"]

    fileOptions: {
      getFileContent: true
    }
  }
]

pipelines: [
  {
    name: "docs-pipeline"
    stages: [
      {
        class: "com.kmwllc.lucille.tika.stage.TextExtractor"
        source: "file_content"
        dest: "extracted_text"
      }
    ]
  }
]
```

Requires the `lucille-tika` Maven dependency. See [TextExtractor]({{< relref "docs/reference/stages/stages_reference" >}}) in All Stages for setup.

## Filter Options Reference

Use `filterOptions` to control which files are processed:

```hocon
filterOptions: {
  # Only process files whose names match these patterns (regex)
  includes: [".*\\.pdf$", ".*\\.docx$"]

  # Skip files whose names match these patterns (regex)
  excludes: [".*\\.DS_Store$", ".*\\.tmp$"]

  # Only process files modified within the last 3 days
  lastModifiedCutoff: "3d"

  # Only process files not published by Lucille in the last 6 hours (requires state)
  lastPublishedCutoff: "6h"
}
```

Duration values accept: `s` (seconds), `m` (minutes), `h` (hours), `d` (days).

## File Options Reference

`fileOptions` controls traversal behavior. `fileHandlers` (a separate block) configures which file types to process and how:

```hocon
fileOptions: {
  # Read file bytes into the file_content field (slow; downloads cloud files)
  getFileContent: true

  # Process zip/tar archives by extracting their contents
  handleArchivedFiles: true

  # Process gzip-compressed files
  handleCompressedFiles: true

  # Move files to this path after successful processing (local files only)
  moveToAfterProcessing: "/data/processed"

  # Move files to this path if an error occurs (local files only)
  moveToErrorFolder: "/data/errors"
}

fileHandlers: {
  csv { ... }
  json { ... }
  xml { ... }
}
```
