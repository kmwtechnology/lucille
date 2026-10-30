---
title: ApplyFileHandlers
weight: 70
date: 2025-06-09
description: Applies FileHandlers to a file referenced by a document, generating child documents for each extracted record.
---

`com.kmwllc.lucille.stage.ApplyFileHandlers`

Applies FileHandlers to the file referenced by `filePathField`, generating a child document for each record the handler extracts (for example, each CSV row or JSON object). The handler is chosen by the extension of the path in `filePathField`, which must match a key in `fileHandlers`. Documents without `filePathField`, or whose extension has no handler, are passed through unchanged. Only the first value of `filePathField` is used.

The file is read from the bytes in `fileContentField` when the document has that field; otherwise it is fetched from the path, which can be a local path, a `classpath:` path, or a cloud storage URI. If the file cannot be fetched or processed, the document fails.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fileHandlers` | Object | Yes | FileHandler configuration, keyed by file extension. Same structure as the `fileHandlers` block in the [FileConnector]({{< relref "docs/ingest-design/connectors/file_connector#file-handlers" >}}). At least one handler must be declared. `csv`, `json` and `jsonl` use Lucille's built-in handlers unless a `class` is given; other extensions require a `class`. Configuring only one of `json` or `jsonl` handles both. The built-in `xml` handler is not supported by this stage, and documents with `.xml` paths fail. |
| `filePathField` | String | No | Field containing the file path or URI. Default: `file_path`. |
| `fileContentField` | String | No | Field containing the file contents as bytes. If present on a document, it is used instead of fetching the file. Default: `file_content`. |
| `s3` | Object | No | S3 credentials for fetching files from S3: `accessKeyId`, `secretAccessKey`, `region`, `anonymous`, `maxResultsPerPage`. See [Cloud Storage Configuration]({{< relref "docs/ingest-design/connectors/file_connector#cloud-storage-configuration" >}}). |
| `azure` | Object | No | Azure credentials for fetching files from Azure: `connectionString`, `accountName`, `accountKey`, `maxResultsPerPage`. See [Cloud Storage Configuration]({{< relref "docs/ingest-design/connectors/file_connector#cloud-storage-configuration" >}}). |
| `gcp` | Object | No | Google Cloud credentials for fetching files from GCS: `pathToServiceKey` (required within `gcp`), `maxResultsPerPage`. See [Cloud Storage Configuration]({{< relref "docs/ingest-design/connectors/file_connector#cloud-storage-configuration" >}}). |
| `storageClients` | Object | No | Custom StorageClients, keyed by URI scheme, for fetching files from other storage systems. See [Storage clients for other URI schemes]({{< relref "docs/ingest-design/connectors/file_connector#cloud-storage-configuration" >}}). |
