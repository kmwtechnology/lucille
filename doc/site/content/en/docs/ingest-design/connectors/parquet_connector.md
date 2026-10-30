---
title: Parquet Connector
weight: 5
date: 2025-06-09
description: A Connector that reads Apache Parquet files and publishes each row as a Lucille Document.
---

[Source Code](https://github.com/kmwtechnology/lucille/blob/main/lucille-plugins/lucille-parquet/src/main/java/com/kmwllc/lucille/parquet/connector/ParquetConnector.java)

The `ParquetConnector` reads [Apache Parquet](https://parquet.apache.org/) files, locally or from Amazon S3, and publishes each row as a Lucille Document. Parquet is a columnar format commonly used to store pre-computed embeddings, feature vectors, and large datasets.

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
| `pathToStorage` | String | Yes | Path to a Parquet file, or a directory that is traversed recursively for Parquet files. |
| `idField` | String | Yes | Column whose value becomes the Document ID. It must exist in each file's schema and must be a string (`BINARY`) column; a missing schema column or a non-string column fails the run. A row whose ID is null or blank is skipped and logged, does not count toward `limit`, and the number of skipped rows is logged when the file is done. |
| `fsUri` | String | Yes | URI of the Hadoop filesystem to use (e.g., `"file:///"` for local, `"s3a://my-bucket"` for S3). `pathToStorage` is resolved on this filesystem. |
| `s3Key` | String | No | AWS access key. Used only when `s3Secret` is also set; otherwise both are ignored and Hadoop's default S3A credential lookup applies. |
| `s3Secret` | String | No | AWS secret key. Used only when `s3Key` is also set. |
| `limit` | Long | No | Maximum number of Documents to publish across all files. Default: no limit. |
| `start` | Long | No | Number of rows to skip at the beginning of each file. Default: `0`. |

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

When both `s3Key` and `s3Secret` are set, the connector configures Hadoop's S3A filesystem with those credentials and path-style access.

## Document Mapping

- Each row becomes one Document. The `idField` column is used as the ID and is not also added as a field.
- The connector's `docIdPrefix` is not applied to IDs.
- Primitive columns of type `BINARY` (read as strings), `INT32`, `INT64`, `FLOAT`, and `DOUBLE` become single-valued fields of the same name. Columns of other primitive types (such as `BOOLEAN`, `INT96` or `FIXED_LEN_BYTE_ARRAY`) are skipped. A column named after a reserved field (such as `run_id`) fails the run.
- Null values in primitive columns are skipped.
- Group (nested) columns are read as Parquet `LIST` columns: each primitive element is added to a multi-valued field named after the column, and elements that are themselves groups are skipped. A row where a list column is null fails the run. Other nested shapes (structs, maps) are not supported and may fail the run or be read incorrectly.

## Notes

- The connector uses Hadoop's filesystem abstraction (`FileSystem`) for path traversal. Unlike `FileConnector`, it does not use Lucille's `StorageClient` infrastructure.
- Only files whose names end in `parquet` are processed.
- A file whose schema lacks `idField` stops the connector with an error.
- An I/O error while reading a row group is logged as a warning and the rest of that file is skipped; the run continues with the next file.
- Local paths (`fsUri` of `file:///`) are listed with Java NIO rather than Hadoop, so no Hadoop native libraries are needed on Windows.
- `start` applies to each file, while `limit` is a total across all files. When paginating with `start`/`limit`, it is recommended to use individual Connectors for each Parquet file rather than a directory path.
