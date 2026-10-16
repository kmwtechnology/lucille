---
title: EmbeddedPython
weight: 290
date: 2025-12-16
description: Run a document through a Java embedded Graal Python environment.
---

`com.kmwllc.lucille.stage.EmbeddedPython`

### Why Use It?

EmbeddedPython executes per-document Python code inside the Lucille JVM using [GraalPy](https://www.graalvm.org/python/). Instead of returning a JSON object, your script mutates the current document directly through a Python-friendly proxy bound as `doc` (and the raw Java `Document` as `rawDoc`). This avoids ports, subprocesses, venvs, and per-document JSON round trips.

### When To Use It

Use EmbeddedPython when you need one or more of the following:
- Minimal operational overhead (ports, subprocess lifecycle, venv creation, pip installs).
- No use of any external Python libraries or native dependencies that require a real Python environment.
- Lightweight field enrichment/transformation.

### When To Use ExternalPython Instead

Avoid EmbeddedPython and use ExternalPython when you need one or more of the following:
- Real Python compatibility (including packages with native dependencies).
- Dependency management via a requirements.txt installed into a managed venv.
- Process isolation apart from the JVM.

### Example

#### Input Document

```json
{
  "id": "doc-1",
  "title": "Hello",
  "author": "Test",
  "views": 123
}
```

#### Python Script

```python
doc["title"] = doc["title"].upper()
```

#### Output Document

```json
{
  "id": "doc-1",
  "title": "HELLO",
  "author": "Test",
  "views": 123
}
```

#### Config Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `scriptPath` | String | No | Path to a Python file. Can be a local path, a `classpath:` path, or a cloud storage URI (`s3://`, `gs://`, `https://` for Azure). Exactly one of `scriptPath` or `script` must be set, or the stage fails at startup. |
| `script` | String | No | Inline Python code. Exactly one of `scriptPath` or `script` must be set. |
| `s3` | Object | No | S3 credentials for reading `scriptPath` from S3: `accessKeyId`, `secretAccessKey`, `region`, `anonymous`, `maxNumOfPages`. See [Cloud Storage Configuration]({{< relref "docs/ingest-design/connectors/file_connector#cloud-storage-configuration" >}}). |
| `azure` | Object | No | Azure credentials for reading `scriptPath` from Azure: `connectionString`, `accountName`, `accountKey`, `maxNumOfPages`. See [Cloud Storage Configuration]({{< relref "docs/ingest-design/connectors/file_connector#cloud-storage-configuration" >}}). |
| `gcp` | Object | No | Google Cloud credentials for reading `scriptPath` from GCS: `pathToServiceKey` (required within `gcp`), `maxNumOfPages`. See [Cloud Storage Configuration]({{< relref "docs/ingest-design/connectors/file_connector#cloud-storage-configuration" >}}). |

The script is read once at startup and evaluated once per document in the same interpreter context, so top-level variables persist between documents. Fields can be read and written as `doc["field"]` or `doc.field`, including nested objects and list indices. Reading a field that does not exist raises an error. Any error raised by the script fails the document.

```hocon
{
 name: "EmbeddedPython-Example"
 class: "com.kmwllc.lucille.stage.EmbeddedPython"

 # Specify exactly one of the following:
 scriptPath: "/path/to/my_script.py"
 script: "doc['title'] = doc['title'].upper()"
}
```

`scriptPath` can begin with `classpath:` to load the script as a classpath resource rather than a filesystem path:

```hocon
scriptPath: "classpath:scripts/my_script.py"
```

This lets you package Python scripts inside your JAR as resources (under `src/main/resources/`). The Lucille process loads them directly from the classpath at runtime, so you don't need to place files in specific filesystem locations or ensure the process has read access to them.
