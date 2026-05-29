---
title: "Control Flow"
weight: 3
date: 2025-05-23
description: >
  How to control what happens to a document as it moves through a pipeline — conditions, skipping, dropping, errors, child documents, and connector sequencing.
---

This page is relevant to both **ingest designers** (working in config only) and **component developers** (writing Java stage and connector code). Each section identifies which approach applies to you.

---

## Conditional Stage Execution

**Config** | **When to use:** You want a stage to run only on documents that have a particular field, or a particular value in a field. For example, only run an embedding stage on documents where `content_type` is `"article"`, or only run a cleanup stage on documents that have a `raw_html` field.

Add a `conditions` block to the stage in your config. The base `Stage` class evaluates conditions before calling `processDocument()` — if the conditions are not met, the stage is skipped for that document entirely. You never need to implement this logic inside a stage.

```hocon
{
  class: "com.kmwllc.lucille.stage.OpenAIEmbed"
  fields: ["content"]
  dest: ["content_vector"]
  apiKey: ${OPENAI_API_KEY}
  conditionPolicy: "all"
  conditions: [
    { fields: ["content"] }
    { fields: ["content_type"], values: ["article"] }
  ]
}
```

See [Stages]({{< relref "docs/reference/stages" >}}) for the full conditions reference including `operator` and `conditionPolicy`.

---

## Skipping a Document

**Config + Code** | **When to use:** You want a document to bypass all remaining pipeline stages but still be sent to the indexer. The canonical use case is a deletion tombstone: a connector emits a document representing a record that should be deleted from the index, and you want it to reach the indexer without being enriched or transformed by downstream stages.

**Config approach:** Add a `SkipDocument` stage to your pipeline with conditions that identify which documents should be skipped.

```hocon
{
  class: "com.kmwllc.lucille.stage.SkipDocument"
  conditions: [
    { fields: ["is_tombstone"], values: ["true"] }
  ]
}
```

**Code approach:** From inside `processDocument()`, call `doc.setSkipped(true)`. All subsequent stages in the pipeline check `isSkipped()` before processing and will skip the document automatically.

In both cases the document still flows to the indexer.

---

## Dropping a Document

**Config + Code** | **When to use:** You want to abandon a document entirely — it should not be processed by downstream stages and should not be sent to the indexer. For example, you might drop documents that fail a quality check, documents that represent records you don't need to reprocess, or documents that are out of scope for the current run.

**Config approach:** Add a `DropDocument` stage to your pipeline with conditions that identify which documents should be dropped.

```hocon
{
  class: "com.kmwllc.lucille.stage.DropDocument"
  conditions: [
    { fields: ["status"], values: ["archived"] }
  ]
}
```

**Code approach:** From inside `processDocument()`, call `doc.setDropped(true)`. The document will not be processed by any subsequent stage and will not be sent to the indexer. Lucille records it as a dropped document in the run summary.

`DropDocument` also accepts an optional `percentage` parameter (a value between 0.0 and 1.0) that drops documents probabilistically. This is primarily useful for testing to simulate document loss in a non-deterministic way.

---

## Sending a Document to an Error State

**Code** | **When to use:** You are writing a stage and something has gone wrong that makes it impossible or unsafe to continue processing the document — for example, a required external service is unavailable, or a field contains data in a format the stage cannot handle. You want the document to stop processing, not be indexed, and be recorded as a failure in the run summary.

Throw a `StageException` from `processDocument()`.

```java
@Override
public Iterator<Document> processDocument(Document doc) throws StageException {
  String value = doc.getString("required_field");
  if (value == null) {
    throw new StageException("required_field is missing on document " + doc.getId());
  }
  // ...
}
```

The framework catches the exception, marks the document as failed, and continues processing other documents. The run summary will report the document as an error.

Use this conservatively. Most stages should not throw `StageException` for ordinary data variation — a missing optional field or an unexpected value is usually better handled by logging or by skipping the transformation for that document. Reserve `StageException` for conditions where continuing to process the document would produce incorrect or corrupt results.

---

## Logging an Error Without Stopping Processing

**Code** | **When to use:** Something unexpected happened while processing a document, but it is not severe enough to stop processing or prevent indexing. You want to record the problem for later investigation — in the logs, in the index itself, or both.

Log the error using the stage's logger, and optionally write a custom field to the document that will be visible in the index.

```java
@Override
public Iterator<Document> processDocument(Document doc) throws StageException {
  try {
    String result = callExternalService(doc.getString("content"));
    doc.setField("enriched_content", result);
  } catch (Exception e) {
    log.error("External service call failed for document {}: {}", doc.getId(), e.getMessage());
    doc.setField("enrichment_error", e.getMessage());
    // processing continues; document will still be indexed
  }
  return null;
}
```

Adding a dedicated error field to the document (like `enrichment_error` above) makes failures searchable and auditable in the index, which is useful when you need to identify and reprocess documents that encountered a specific problem.

---

## Attaching a Child Document

**Code** | **When to use:** You want to associate additional structured data with a document — for example, metadata extracted from a file, or sub-records parsed from a parent record — but you do not want those sub-records to be processed independently by downstream stages or indexed as separate documents. The child data travels with the parent and is accessible to stages that specifically look for it.

Call `doc.addChild(childDoc)` from inside `processDocument()` and return `null`.

```java
@Override
public Iterator<Document> processDocument(Document doc) throws StageException {
  Document child = Document.create(doc.getId() + "-metadata");
  child.setField("extracted_author", parseAuthor(doc.getString("raw_header")));
  doc.addChild(child);
  return null;
}
```

Attached children are stored inside the parent under the reserved `___children` field. Ordinary stages ignore them. Only stages that explicitly iterate `doc.getChildren()` will see them. They are not independently tracked by the Publisher and are not indexed as separate records unless a subsequent `EmitNestedChildren` stage promotes them.

---

## Emitting a Child Document

**Config + Code** | **When to use:** You want to produce additional documents that flow through the remaining pipeline stages independently and are indexed as separate records. The canonical use case is chunking: a single large document is split into many smaller chunks, each of which needs to be embedded and indexed on its own. More generally, any time a stage produces attached children (via `doc.addChild()`) and you want those children to become independent pipeline documents, you need to emit them.

**Config approach:** Add an `EmitNestedChildren` stage to your pipeline after any stage that attaches children. It detaches the children from the parent and emits them as independent documents. If the parent document itself should not be indexed, set `dropParent: true`. Use `fieldsToCopy` to copy fields from the parent to each child before they separate — useful for propagating metadata like a title or source URL.

```hocon
{
  class: "com.kmwllc.lucille.stage.EmitNestedChildren"
  dropParent: true
  fieldsToCopy: {
    "title": "parent_title"
    "source_url": "source_url"
  }
}
```

`EmitNestedChildren` is a no-op if the document has no attached children, so it is safe to place in a pipeline where only some documents will have children. See [ChunkText]({{< relref "docs/reference/stages/chunk_text" >}}) for the common chunking pattern that uses this stage.

**Code approach:** Return an `Iterator<Document>` directly from `processDocument()`. Children returned this way become independent pipeline documents immediately — they are tracked by the Publisher, processed by all downstream stages, and indexed separately. No `EmitNestedChildren` stage is needed.

```java
@Override
public Iterator<Document> processDocument(Document doc) throws StageException {
  List<String> chunks = chunk(doc.getString("body"));
  List<Document> children = new ArrayList<>();
  for (int i = 0; i < chunks.size(); i++) {
    Document child = Document.create(doc.getId() + "-chunk-" + i);
    child.setField("text", chunks.get(i));
    child.setField("parent_id", doc.getId());
    children.add(child);
  }
  return children.iterator();
}
```

---

## Running Connectors in Sequence

**Config** | **When to use:** You have two or more connectors that must run in a specific order, and the second should only run if the first succeeds. For example, a connector that deletes stale records from the index followed by a connector that re-ingests fresh records — you don't want the re-ingest to proceed if the deletion step failed.

List the connectors in order in the `connectors` array of a single Lucille config. Lucille runs connectors sequentially and aborts the run if any connector fails (meaning any of its lifecycle methods throw an exception). A connector is not considered failed if individual documents it publishes encounter errors during pipeline processing.

```hocon
connectors: [
  {
    name: "delete-stale"
    class: "com.kmwllc.lucille.connector.SolrConnector"
    // ...
  },
  {
    name: "ingest-fresh"
    class: "com.kmwllc.lucille.connector.FileConnector"
    pipeline: "my-pipeline"
    // ...
  }
]
```

Note that all connectors in a single config share the same indexer. If your connectors need to write to different backends, use separate configs.

---

## Running Connectors in Parallel

**Config** | **When to use:** You have two independent ingests that do not depend on each other and you want them to run at the same time to reduce total wall-clock time.

Create two separate Lucille config files and launch them as separate processes from the command line. Each process runs its own Runner, its own pipeline workers, and its own indexer connection independently.

```bash
java -Dconfig.file=ingest-a.conf -cp '...' com.kmwllc.lucille.core.Runner &
java -Dconfig.file=ingest-b.conf -cp '...' com.kmwllc.lucille.core.Runner &
```

There is no built-in mechanism for parallel connector execution within a single Lucille process. If the two ingests write to the same index, ensure their document IDs do not collide.

---

## Pre- and Post-Connector Actions

**Config + Code** | **When to use:** You need to perform setup or teardown work that is tightly coupled to a connector's execution — for example, deleting stale records from a search index before a connector re-ingests them, or committing the index after ingestion completes. You want this work to be part of the connector's lifecycle so that failures in setup prevent the connector from running at all.

**Config approach:** `SolrConnector` is the canonical example. It accepts `preActions` and `postActions` lists in its config — arbitrary Solr update requests (JSON or XML) that are issued before and after the connector queries Solr. The `{runId}` placeholder in action strings is substituted with the current run ID at execution time.

```hocon
{
  name: "solr-reindex"
  class: "com.kmwllc.lucille.connector.SolrConnector"
  pipeline: "my-pipeline"
  solr: { url: "http://localhost:8983/solr/mycore" }
  preActions: [
    "{\"delete\": {\"query\": \"runId:{runId}\"}}"
  ]
  postActions: [
    "{\"commit\": {}}"
  ]
}
```

**Code approach:** Override `preExecute(String runId)` and/or `postExecute(String runId)` in your connector implementation. The lifecycle contract is:

- `preExecute()` is always called first. If it throws a `ConnectorException`, `execute()` and `postExecute()` are skipped.
- `execute()` is called if `preExecute()` succeeds. If it throws, `postExecute()` is skipped.
- `postExecute()` is called only if both `preExecute()` and `execute()` succeed.
- `close()` is always called regardless of what happened above.

Keep resource cleanup (closing connections, releasing file handles) in `close()`, not `postExecute()`. `postExecute()` is for post-success actions; `close()` is the guaranteed teardown.

---

## Stage Initialization and Teardown

**Code** | **When to use:** You are writing a stage that needs to acquire resources before processing begins — opening a database connection, building an HTTP client, loading a model file, compiling a regex or expression, or validating configuration values that can only be checked at runtime. Override `start()` and/or `stop()` to manage that lifecycle cleanly.

### start()

`start()` is called once per worker thread, after the stage is constructed but before any documents are processed. Because each worker thread gets its own pipeline instance with its own stage instances, resources allocated in `start()` are effectively thread-local — no synchronization is needed.

Use `start()` for:

- **Opening connections** — database connections, HTTP clients, external service clients. `QueryDatabase` opens its JDBC connection and prepares its SQL statement here; `FetchUri` builds its `CloseableHttpClient` here.
- **Loading or compiling resources** — parsing expressions, loading NLP models, reading lookup files. `ApplyJSONata` compiles its JSONata expression here; `ChunkText` loads its OpenNLP sentence model here.
- **Runtime config validation** — checks that can't be done in the constructor because they depend on the interaction of multiple config values, or because they involve I/O. `FetchUri` validates its `statusCodeRetryList` here and throws `StageException` if the configuration would cause infinite retries. `RenameFields` throws if `fieldMapping` is empty.

Throwing `StageException` from `start()` prevents the pipeline from starting at all for that worker thread, which is the right behavior when the stage cannot function without the resource it failed to acquire.

```java
@Override
public void start() throws StageException {
  try {
    this.connection = DriverManager.getConnection(connectionString, jdbcUser, jdbcPassword);
    this.preparedStatement = connection.prepareStatement(sql);
  } catch (SQLException e) {
    throw new StageException("Could not connect to database", e);
  }
}
```

### stop()

`stop()` is called once per worker thread after all documents have been processed, during pipeline shutdown. Use it to release whatever `start()` acquired.

Use `stop()` for:

- **Closing connections** — JDBC connections, HTTP clients, script engine contexts. `QueryDatabase` closes its connection and prepared statement here; `FetchUri` closes its HTTP client here; `ApplyJavascript` closes its GraalVM context here.
- **Flushing output** — `Print` closes its writer here if one was opened.

```java
@Override
public void stop() throws StageException {
  try {
    if (connection != null) connection.close();
    if (preparedStatement != null) preparedStatement.close();
  } catch (SQLException e) {
    throw new StageException("Error closing database resources", e);
  }
}
```

### What belongs in the constructor vs. start()

The constructor runs once when the stage is instantiated (before worker threads are assigned). `start()` runs once per worker thread just before processing begins. In practice:

- **Constructor:** Read config values into instance fields. Do not open connections or load large resources here — the constructor is called during pipeline validation and startup, before workers are ready.
- **start():** Open connections, load resources, compile expressions, validate runtime constraints.
- **stop():** Release everything acquired in `start()`.
- **processDocument():** Use the resources prepared in `start()`. Do not open or close connections per document.
