---
weight: 1
title: Document
date: 2025-10-23
description: The basic unit of data that is sent through a Pipeline and eventually indexed into a search engine.
---

A Lucille *Document* is the basic unit of data that flows through a pipeline and gets indexed.

A Document is an ordered, named set of fields. Each field may hold a **single value** or a **list of values** (multi-valued). All field values are ultimately represented in JSON. Every Document has a unique `id`.

## Creating a Document

Use the static factory methods on the `Document` interface:

```java
// Create with an explicit ID
Document doc = Document.create("my-doc-123");

// Create with an auto-generated UUID
Document doc = Document.create();
```

Connectors typically create Documents and call `publisher.publish(doc)` to send them into the pipeline.

## Reserved Fields

Lucille reserves several field names for internal use. Do not use these for application data.

| Field | Description |
|---|---|
| `id` | The unique document ID. Immutable once set. |
| `run_id` | The run ID stamped by the Publisher. Immutable once set. |
| `___children` | Internal: tracks child documents generated in the pipeline. |
| `___dropped` | Set to `true` when a document is dropped (not sent to the Indexer). |
| `___skipped` | Set to `true` when a document should bypass Stages but still reach the Indexer (used for deletions). |

## Field Types

Lucille Documents support the following value types:

- `String`
- `Boolean`
- `Integer`
- `Double`
- `Float`
- `Long`
- `java.time.Instant`
- `byte[]`
- `com.fasterxml.jackson.databind.JsonNode`
- `java.sql.Timestamp`
- `java.util.Date`

## Reading Fields

### Single-Valued Access

```java
String  title  = doc.getString("title");
int     count  = doc.getInt("count");
double  score  = doc.getDouble("score");
float   weight = doc.getFloat("weight");
long    size   = doc.getLong("size");
boolean flag   = doc.getBoolean("active");
Instant ts     = doc.getInstant("created_at");
byte[]  raw    = doc.getBytes("content");
JsonNode json  = doc.getJson("metadata");
```

### Multi-Valued Access

These methods also wrap a single value in a list if needed:

```java
List<String>  titles = doc.getStringList("title");
List<Integer> counts = doc.getIntList("counts");
List<Double>  scores = doc.getDoubleList("scores");
List<Long>    sizes  = doc.getLongList("sizes");
```

### Checking Field Existence

```java
if (doc.has("title")) {
    String t = doc.getString("title");
}
```

## Writing Fields

### `setField` — Overwrite

Replaces any existing value(s) and makes the field single-valued:

```java
doc.setField("title", "Hello World");
doc.setField("count", 42);
doc.setField("active", true);
doc.setField("score", 0.95);
doc.setField("created_at", Instant.now());
```

### `addToField` — Append

Appends a value, converting the field to multi-valued if it was single-valued:

```java
doc.addToField("tags", "search");
doc.addToField("tags", "etl");
// tags is now ["search", "etl"]
```

### `setOrAdd` — Create or Append

Creates the field as single-valued if it does not exist; appends if it does:

```java
doc.setOrAdd("tags", "search");
doc.setOrAdd("tags", "etl");
```

### `update` — Controlled Write with UpdateMode

The `update` method accepts an `UpdateMode` enum that covers the three most common write patterns:

```java
import com.kmwllc.lucille.core.UpdateMode;

// OVERWRITE: first value replaces all existing values; additional values are appended
doc.update("title", UpdateMode.OVERWRITE, "New Title");

// APPEND: all values are appended (field becomes or remains multi-valued)
doc.update("tags", UpdateMode.APPEND, "search", "etl");

// SKIP: field is left unchanged if it already has a value
doc.update("title", UpdateMode.SKIP, "Default Title");
```

## Nested JSON

Documents support reading and writing values within nested JSON objects and arrays using **dot-path notation** (e.g., `"metadata.author.name"`) or structured `List<Document.Segment>` paths.

### Reading Nested Values

```java
JsonNode node = doc.getNestedJson("metadata.author.name");

// Or using structured path segments
List<Document.Segment> path = Document.Segment.parse("metadata.items[2].title");
JsonNode node2 = doc.getNestedJson(path);
```

### Writing Nested Values

```java
ObjectMapper mapper = new ObjectMapper();
doc.setNestedJson("metadata.score", mapper.valueToTree(0.95));
```

### Removing Nested Values

```java
doc.removeNestedJson("metadata.tempField");
```

### Path Segments

```java
List<Document.Segment> segments = Document.Segment.parse("a.b[2].c");
String path = Document.Segment.stringify(segments); // "a.b[2].c"
```

## Dropping and Skipping

**Dropping** removes the document from the pipeline entirely. It will not reach the Indexer.

Use the `DropDocument` Stage in config, or set the field directly:

```java
doc.setField(Document.DROPPED_FIELD, true);
```

**Skipping** causes the document to bypass all downstream Stages but still reach the Indexer. This is used for deletion markers, so the Indexer can issue a delete against the search backend.

Use the `SkipDocument` Stage in config, or set the field directly:

```java
doc.setField(Document.SKIPPED_FIELD, true);
```

## Child Documents

A Stage may generate **child documents** — additional Documents that flow through the remaining pipeline stages as independent records and are indexed alongside the parent. A Stage returns children from `processDocument()` as an `Iterator<Document>`.

Children are always emitted before the parent document, ensuring the Publisher's accounting registers child IDs before it sees the parent's completion event.

## Iterating Fields

```java
for (String fieldName : doc) {
    // iterate over all field names in the Document
}
```

## Serialization

Documents serialize to and from JSON. In Kafka-distributed mode, Documents flow between components as JSON bytes. The `id` and `run_id` are always included.

```java
String json = doc.toString();
```
