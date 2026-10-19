---
title: ApplyJavascript
weight: 80
date: 2025-06-09
description: Runs a JavaScript snippet per document using GraalVM's JavaScript engine.
---

`com.kmwllc.lucille.stage.ApplyJavascript`

Runs a JavaScript snippet per document using GraalVM's JavaScript engine. The script is read once at startup and evaluated once per document. The document is bound as `doc` (a proxy for reading and writing fields) and as `rawDoc` (the underlying Java `Document`).

| Parameter | Type | Required | Description |
|---|---|---|---|
| `script` | String | No | Inline JavaScript code. |
| `scriptPath` | String | No | Path to a `.js` file. Can be a local path, a `classpath:` path, or a cloud storage URI (`s3://`, `gs://`, `https://` for Azure). Exactly one of `script` or `scriptPath` must be provided, or the stage fails at startup. |
| `s3` | Object | No | S3 credentials for reading `scriptPath` from S3: `accessKeyId`, `secretAccessKey`, `region`, `anonymous`, `maxResultsPerPage`. See [Cloud Storage Configuration]({{< relref "docs/ingest-design/connectors/file_connector#cloud-storage-configuration" >}}). |
| `azure` | Object | No | Azure credentials for reading `scriptPath` from Azure: `connectionString`, `accountName`, `accountKey`, `maxResultsPerPage`. See [Cloud Storage Configuration]({{< relref "docs/ingest-design/connectors/file_connector#cloud-storage-configuration" >}}). |
| `gcp` | Object | No | Google Cloud credentials for reading `scriptPath` from GCS: `pathToServiceKey` (required within `gcp`), `maxResultsPerPage`. See [Cloud Storage Configuration]({{< relref "docs/ingest-design/connectors/file_connector#cloud-storage-configuration" >}}). |

Field access through `doc`:
- Read and write root fields with `doc.field`. Reading a missing field returns `null`. Assigning `null` stores a JSON null.
- Nested objects use dot notation (`doc.a.b`). Parent objects are not created automatically; initialize them first (`doc.a = doc.a || {};`).
- Nested arrays use bracket indices (`doc.a.list[1]`). An array in a root field (such as a multi-valued field) is read as a copy, so `doc.tags[0] = ...` or `doc.tags.push(...)` does not change the document; assign the whole array instead (`doc.tags = [...]`).
- `delete doc.field` removes a field. Deleting an element of a nested array (`delete doc.a.list[1]`) removes it and shifts the remaining elements.

Any error thrown by the script fails the document.

```hocon
{ class: "com.kmwllc.lucille.stage.ApplyJavascript", script: "doc.title = doc.title.toUpperCase();" }
```
