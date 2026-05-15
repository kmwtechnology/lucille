---
title: Plugins
date: 2025-06-09
description: Optional extension modules that add connectors, stages, and indexers with heavyweight dependencies.
weight: 20
---

Lucille's plugin system allows optional components with large or specialized dependencies to be packaged separately from `lucille-core`. This keeps the core JAR small and avoids transitive dependency conflicts for users who don't need every feature.

Each plugin is a separate Maven module under `lucille-plugins/`. Add the plugin as a Maven dependency to include it in your build.

## Available Plugins

### lucille-tika — Text Extraction

Adds the `TextExtractor` Stage, which uses [Apache Tika](https://tika.apache.org/) to extract text from over 1,000 file formats including PDF, Microsoft Office documents, HTML, images (with embedded OCR), and many more.

**Stage:** `com.kmwllc.lucille.tika.stage.TextExtractor`

**Maven dependency:**
```xml
<dependency>
  <groupId>com.kmwllc</groupId>
  <artifactId>lucille-tika</artifactId>
  <version>${lucille.version}</version>
</dependency>
```

**Config example:**
```hocon
{
  name: "extract-text"
  class: "com.kmwllc.lucille.tika.stage.TextExtractor"
  source: "file_content"
  dest: "extracted_text"
}
```

---

### lucille-ocr — Optical Character Recognition

Adds the `ApplyOCR` Stage, which applies OCR to image fields using [Tesseract](https://tesseract-ocr.github.io/).

**Stage:** `com.kmwllc.lucille.ocr.stage.ApplyOCR`

**Maven dependency:**
```xml
<dependency>
  <groupId>com.kmwllc</groupId>
  <artifactId>lucille-ocr</artifactId>
  <version>${lucille.version}</version>
</dependency>
```

**Config example:**
```hocon
{
  name: "ocr"
  class: "com.kmwllc.lucille.ocr.stage.ApplyOCR"
  source: "image_bytes"
  dest: "ocr_text"
  language: "eng"
}
```

Requires Tesseract to be installed on the system running Lucille.

---

### lucille-entity-extraction — Named Entity Recognition

Adds the `ApplyOpenNLPNameFinders` Stage, which performs named entity recognition (NER) using [Apache OpenNLP](https://opennlp.apache.org/) models.

**Stage:** `com.kmwllc.lucille.entity.stage.ApplyOpenNLPNameFinders`

**Maven dependency:**
```xml
<dependency>
  <groupId>com.kmwllc</groupId>
  <artifactId>lucille-entity-extraction</artifactId>
  <version>${lucille.version}</version>
</dependency>
```

---

### lucille-jlama — Local Embedding Generation

Adds the `JlamaEmbed` Stage, which generates vector embeddings using a quantized LLM running locally inside the JVM via [Jlama](https://github.com/tjake/Jlama). No API key or external service required — the model runs directly in the Lucille process.

**Stage:** `com.kmwllc.lucille.jlama.stage.JlamaEmbed`

**Maven dependency:**
```xml
<dependency>
  <groupId>com.kmwllc</groupId>
  <artifactId>lucille-jlama</artifactId>
  <version>${lucille.version}</version>
</dependency>
```

**Config example:**
```hocon
{
  name: "embed"
  class: "com.kmwllc.lucille.jlama.stage.JlamaEmbed"
  source: "content"
  dest: "content_vector"
  modelPath: "/models/my-embedding-model"
}
```

`JlamaEmbed` is useful for teams with data-residency or compliance constraints that prevent sending documents to external APIs like OpenAI.

---

### lucille-parquet — Apache Parquet

Adds the `ParquetConnector`, which reads [Apache Parquet](https://parquet.apache.org/) files and publishes each row as a Lucille Document.

**Connector:** `com.kmwllc.lucille.parquet.connector.ParquetConnector`

**Maven dependency:**
```xml
<dependency>
  <groupId>com.kmwllc</groupId>
  <artifactId>lucille-parquet</artifactId>
  <version>${lucille.version}</version>
</dependency>
```

**Config example:**
```hocon
connectors: [
  {
    name: "parquet-source"
    class: "com.kmwllc.lucille.parquet.connector.ParquetConnector"
    pipeline: "my-pipeline"
    path: "/data/embeddings.parquet"
    idField: "doc_id"
  }
]
```

---

### lucille-pinecone — Pinecone Vector Database

Adds the `PineconeIndexer`, which indexes vector embeddings into [Pinecone](https://www.pinecone.io/).

**Indexer:** `com.kmwllc.lucille.pinecone.indexer.PineconeIndexer`

**Maven dependency:**
```xml
<dependency>
  <groupId>com.kmwllc</groupId>
  <artifactId>lucille-pinecone</artifactId>
  <version>${lucille.version}</version>
</dependency>
```

**Config example:**
```hocon
indexer {
  class: "com.kmwllc.lucille.pinecone.indexer.PineconeIndexer"
  deletionMarkerField: "is_deleted"
  deletionMarkerFieldValue: "true"
}

pinecone {
  apiKey: ${PINECONE_API_KEY}
  index: "my-index"
  vectorField: "content_vector"
  namespace: "default"
}
```

---

### lucille-weaviate — Weaviate Vector Database

Adds the `WeaviateIndexer`, which indexes documents into [Weaviate](https://weaviate.io/).

**Indexer:** `com.kmwllc.lucille.weaviate.indexer.WeaviateIndexer`

**Maven dependency:**
```xml
<dependency>
  <groupId>com.kmwllc</groupId>
  <artifactId>lucille-weaviate</artifactId>
  <version>${lucille.version}</version>
</dependency>
```

---

### lucille-video — Video Frame Extraction

Adds the `VideoFileHandler`, which extracts frames from video files and emits one Document per sampled frame with timing and dimension metadata. Enables frame-level and multimodal search use cases.

**FileHandler:** `com.kmwllc.lucille.video.fileHandler.VideoFileHandler`

**Maven dependency:**
```xml
<dependency>
  <groupId>com.kmwllc</groupId>
  <artifactId>lucille-video</artifactId>
  <version>${lucille.version}</version>
</dependency>
```

Requires FFmpeg to be installed on the system.

---

### lucille-api — REST API

Adds an HTTP REST API built on [Dropwizard](https://www.dropwizard.io/) and JAX-RS that allows managing configs and triggering runs over HTTP rather than via the CLI. Includes Swagger UI and optional basic authentication.

**Maven dependency:**
```xml
<dependency>
  <groupId>com.kmwllc</groupId>
  <artifactId>lucille-api</artifactId>
  <version>${lucille.version}</version>
</dependency>
```

#### Endpoints

All endpoints are under the `/v1` prefix.

**Config management**

| Method | Path | Description |
|---|---|---|
| `POST` | `/v1/config` | Submit a config as a JSON object. Returns a `configId` UUID. |
| `GET` | `/v1/config` | List all stored configs. |
| `GET` | `/v1/config/{configId}` | Retrieve a specific config by ID. |

**Run management**

| Method | Path | Description |
|---|---|---|
| `POST` | `/v1/run` | Start a run. Request body: `{"configId": "<uuid>"}`. Returns `RunDetails`. |
| `GET` | `/v1/run` | List all runs and their status. |
| `GET` | `/v1/run/{runId}` | Get details for a specific run. |

**Health and observability**

| Method | Path | Description |
|---|---|---|
| `GET` | `/v1/livez` | Liveness check — returns 200 if the service is running. |
| `GET` | `/v1/readyz` | Readiness check — returns 200 if the service is ready. |
| `GET` | `/v1/systemstats` | CPU, RAM, JVM heap, and disk usage as JSON. |
| `GET` | `/v1/systemstats/metrics` | Dropwizard Codahale metrics registry as JSON. |

The typical workflow is: `POST /v1/config` to register a config → `POST /v1/run` with the returned `configId` to start a run → poll `GET /v1/run/{runId}` for status.

---

## Adding a Plugin Dependency

Add the plugin to your project's `pom.xml`:

```xml
<dependencies>
  <dependency>
    <groupId>com.kmwllc</groupId>
    <artifactId>lucille-tika</artifactId>
    <version>0.9.0</version>
  </dependency>
</dependencies>
```

Or use the Lucille BOM to manage versions centrally:

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>com.kmwllc</groupId>
      <artifactId>lucille-bom</artifactId>
      <version>0.9.0</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>
```

## Building a Custom Plugin

New connectors, stages, and indexers can be added as separate Maven modules without touching the core project. The plugin mechanism is the same one used by all built-in plugins.

See [Developing New Components]({{< relref "docs/developer-guide/dev_new_components" >}}) for a step-by-step guide.
