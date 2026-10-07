---
title: Parquet Connector
weight: 5
date: 2025-06-09
description: A Connector that reads Apache Parquet files and publishes each row as a Lucille Document.
---

[Source Code](https://github.com/kmwtechnology/lucille/blob/main/lucille-plugins/lucille-parquet/src/main/java/com/kmwllc/lucille/parquet/connector/ParquetConnector.java)

The `ParquetConnector` reads [Apache Parquet](https://parquet.apache.org/) files — locally or from Amazon S3 — and publishes each row as a Lucille Document. Parquet is a columnar format commonly used to store pre-computed embeddings, feature vectors, and large datasets.

**Maven dependency:**
```xml
<dependency>
  <groupId>com.kmwllc</groupId>
  <artifactId>lucille-parquet</artifactId>
  <version>${lucille.version}</version>
</dependency>
```

## Configuration

```hocon
connectors: [
  {
    name: "parquet-source"
    class: "com.kmwllc.lucille.parquet.connector.ParquetConnector"
    pipeline: "my-pipeline"
    pathToStorage: "/data/embeddings.parquet"
    idField: "doc_id"
    fsUri: "file:///"
  }
]
```

## Configuration Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `pathToStorage` | String | Yes | Path to a Parquet file or directory to traverse for `.parquet` files. |
| `idField` | String | Yes | Field name in the Parquet schema to use as the Document ID. Must exist in the file's schema. |
| `fsUri` | String | Yes | URI for the filesystem to use (e.g., `"file:///"` for local, `"s3a://my-bucket"` for S3). |
| `s3Key` | String | No | AWS S3 access key. Required when using S3. |
| `s3Secret` | String | No | AWS S3 secret key. Required when using S3. |
| `limit` | Long | No | Maximum number of Documents to publish. Default: no limit. |
| `start` | Long | No | Number of rows to skip from the beginning of each file. Default: `0`. |

## S3 Configuration

For S3, provide the filesystem URI and credentials:

```hocon
connectors: [
  {
    name: "parquet-s3"
    class: "com.kmwllc.lucille.parquet.connector.ParquetConnector"
    pipeline: "my-pipeline"
    pathToStorage: "/prefix/embeddings"
    idField: "doc_id"
    fsUri: "s3a://my-bucket"
    s3Key: ${AWS_ACCESS_KEY_ID}
    s3Secret: ${AWS_SECRET_ACCESS_KEY}
  }
]
```

## Notes

- The connector uses Hadoop's filesystem abstraction (`FileSystem`) for path traversal. Unlike `FileConnector`, it does not use Lucille's `StorageClient` infrastructure.
- Parquet files must have the `.parquet` extension to be processed.
- When paginating with `start`/`limit`, it is recommended to use individual Connectors for each Parquet file rather than a directory path.
- The Parquet format requires random-access reads (not sequential streaming), which is why it is implemented as a standalone Connector rather than a `FileConnector` FileHandler.
