---
title: FetchFileContent
weight: 350
date: 2025-06-09
description: Loads the content of a file path into a byte array field on the document.
---

`com.kmwllc.lucille.stage.FetchFileContent`

Loads the content of the file at the path in `filePathField` into `fileContentField` as a byte array, overwriting any existing value. The path can be a local path, a `classpath:` path, or a cloud storage URI. Documents without `filePathField` are passed through unchanged. Only the first value of `filePathField` is used. If the file cannot be read, or is larger than `maxSizeBytes`, the document fails and `fileContentField` is left untouched.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `filePathField` | String | No | Field containing the file path or URI. Default: `file_path`. |
| `fileContentField` | String | No | Destination byte array field. Default: `file_content`. |
| `maxSizeBytes` | Long | No | Maximum file size to load; must be positive and at most 2147483638, or the stage fails at startup. Larger files cause a StageException. Default: unlimited. |
| `s3` | Object | No | S3 credentials for reading files from S3: `accessKeyId`, `secretAccessKey`, `region`, `anonymous`, `maxResultsPerPage`. See [Cloud Storage Configuration]({{< relref "docs/ingest-design/connectors/file_connector#cloud-storage-configuration" >}}). |
| `azure` | Object | No | Azure credentials for reading files from Azure: `connectionString`, `accountName`, `accountKey`, `maxResultsPerPage`. See [Cloud Storage Configuration]({{< relref "docs/ingest-design/connectors/file_connector#cloud-storage-configuration" >}}). |
| `gcp` | Object | No | Google Cloud credentials for reading files from GCS: `pathToServiceKey` (required within `gcp`), `maxResultsPerPage`. See [Cloud Storage Configuration]({{< relref "docs/ingest-design/connectors/file_connector#cloud-storage-configuration" >}}). |
| `storageClients` | Object | No | Custom StorageClients, keyed by URI scheme, for reading files from other storage systems. See [Storage clients for other URI schemes]({{< relref "docs/ingest-design/connectors/file_connector#cloud-storage-configuration" >}}). |
