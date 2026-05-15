---
title: Document Model
weight: 1
date: 2025-06-09
description: >
  Why the Document is backed by a Jackson ObjectNode, the API design choices, and the tradeoffs.
---

## Why the Document API Matters

At first glance, the idea of a "document" in a search ingestion system seems simple: it's a bag of named fields with values. A `Map<String, Object>` would seem to suffice. In practice, a well-designed Document API turns out to be one of the most important practical considerations in a search ETL framework, for reasons that only become apparent when you've written dozens of pipeline stages and dealt with the realities of search engine field models.

Every pipeline stage reads fields, transforms them, and writes results back. A stage might be three lines of logic surrounded by ten lines of field access boilerplate — checking if a field exists, handling null, deciding whether to overwrite or append, converting types, dealing with single-valued vs. multi-valued fields. If the Document API is clumsy, that boilerplate dominates every stage you write. If the API is well-designed, stages are concise and the intent is clear.

Lucille's Document API is the result of iterating on this problem across many real-world pipelines. Every method exists because a common pattern in search ingestion code demanded it.

---

## Matching the Search Engine's Field Model

Search engines (Solr, Elasticsearch, OpenSearch) have a field model that differs from a typical programming language map in important ways:

**Single-valued vs. multi-valued fields.** In a search engine schema, a field can hold one value or a list of values. A document might have a single `title` but multiple `tags`. This distinction matters for how the field is indexed, how it's queried, and how it's displayed. A `Map<String, Object>` does not capture this distinction — is the value a `String` or a `List<String>`? You'd need to check at every access point.

Lucille's Document makes this explicit:
- `getString("title")` returns the single value (or the first value if multi-valued).
- `getStringList("tags")` returns all values as a list (wrapping a single value in a list if necessary).
- `setField("title", "Hello")` creates a single-valued field.
- `addToField("tags", "search")` converts the field to multi-valued if it wasn't already.
- `setOrAdd("tags", "etl")` creates the field as single-valued if absent, or appends if present.

This mirrors exactly how search engines think about fields. A stage author doesn't need to write `if (value instanceof List)` checks — the API handles the single/multi distinction uniformly.

**The three update patterns.** When a stage writes to a field, there are exactly three things it might want to do:
1. **Overwrite** whatever was there before.
2. **Append** to whatever was there (creating a multi-valued field).
3. **Skip** — write only if the field doesn't already exist (don't clobber earlier enrichment).

These three patterns appear so frequently in search ingestion code that Lucille provides them as a first-class `UpdateMode` enum, usable with the `update()` method:

```java
doc.update("title", UpdateMode.OVERWRITE, "New Title");
doc.update("tags", UpdateMode.APPEND, "tag1", "tag2");
doc.update("summary", UpdateMode.SKIP, "Default summary");
```

The `update()` method also accepts varargs, so a stage can write multiple values in a single call. Without this, every stage would implement its own if/else logic for these three cases — and get it subtly wrong in edge cases (e.g., forgetting to convert a single-valued field to multi-valued before appending).

---

## Why JSON Backing (ObjectNode) Is the Right Choice

Lucille's `Document` is implemented as a thin wrapper around a Jackson `ObjectNode`. This is not an obvious choice — a `HashMap<String, Object>` would be simpler to implement. The JSON backing is motivated by the realities of how documents flow through the system.

### Serialization Without Type Metadata

Documents cross boundaries constantly in Lucille: they're placed on queues (in-memory or Kafka), sent to search backends via bulk APIs, logged for debugging, and captured in test mode for assertions. Every boundary crossing requires serialization.

With a JSON-backed document, serialization is trivial: `data.toString()` produces valid JSON. No conversion step, no schema, no type registry.

Critically, **JSON's type system eliminates the need to store explicit type information with each field.** Jackson's ObjectNode stores values as typed nodes — `TextNode`, `IntNode`, `BooleanNode`, `ArrayNode`, etc. When serialized to JSON, the types are implicit in the syntax:
- `"title": "Hello"` — string (quoted)
- `"count": 42` — integer (unquoted number)
- `"active": true` — boolean (literal)
- `"tags": ["a", "b"]` — array (brackets)

When deserialized, Jackson reconstructs the correct node types from the JSON syntax. No type annotations, no class names in the serialized form, no versioning concerns. Compare this to Java serialization or a HashMap-based approach where you'd need to store type discriminators alongside values to reconstruct them correctly on the other end.

This matters operationally: documents on Kafka topics are human-readable JSON. An administrator can inspect them with standard Kafka tooling and understand what they contain without a decoder ring.

### Zero-Cost Boundary Crossings

Because the document *is* JSON internally, there is no impedance mismatch at any boundary:

- **From a JSON source (file, HTTP response, Kafka message):** Parse the JSON into an ObjectNode and it becomes the Document's backing store directly. No field-by-field copying.
- **To a search engine:** The bulk API for Elasticsearch, OpenSearch, and Solr all accept JSON. The document is already in the right format.
- **To Kafka:** Serialize the ObjectNode to a string. Done.
- **From Kafka:** Parse the string back to an ObjectNode. Done.
- **In test assertions:** `document.toString()` gives you the complete state as readable JSON.

A HashMap-backed document would require a serialization pass at every one of these boundaries — and in a system where a document crosses 4+ boundaries (source → processing queue → worker → indexing queue → indexer → search backend), that overhead is significant.

### Native Nested Structure Support

Modern search ingestion often involves complex nested data: JSON responses from HTTP enrichment stages, structured extraction results from LLMs, nested document schemas in Elasticsearch. An ObjectNode naturally represents nested JSON (objects within objects, arrays of objects).

Lucille's `getNestedJson`/`setNestedJson` API works directly on the tree structure:

```java
// Read a nested value
JsonNode author = doc.getNestedJson("metadata.author.name");

// Set a nested value (creates intermediate objects as needed)
doc.setNestedJson("metadata.source.url", TextNode.valueOf("https://..."));

// Array indexing
JsonNode thirdTag = doc.getNestedJson("results[0].metadata.tags[2]");
```

With a HashMap, nested access would require `((Map) ((Map) map.get("metadata")).get("author")).get("name")` — casting at every level, null-checking at every level, and no type safety. The Jackson tree API provides a typed, null-safe traversal.

### Interop with the Jackson Ecosystem

Stages that call external APIs (HTTP enrichment, LLM calls, search engine queries) typically use HTTP clients that return Jackson `JsonNode` objects. These can be stored directly on the Document without conversion:

```java
JsonNode apiResponse = httpClient.get(url);  // returns JsonNode
doc.setField("enrichment_result", apiResponse);  // stored directly
```

Similarly, Lucille's JSONata transformation support operates on the Jackson tree natively — the Document's backing ObjectNode is the input to the JSONata expression, and the result is written back as a JsonNode.

---

## The Tradeoffs

The JSON backing is not free:

**Per-field access overhead.** Getting a String from an ObjectNode means `node.get("field").asText()` rather than `(String) map.get("field")`. The Document API hides this behind typed getters, but the implementation does more work per access than a HashMap. For stages that read many fields in a tight loop, this is measurably slower.

**Memory overhead.** Each field value is wrapped in a JsonNode subclass (TextNode, IntNode, etc.) rather than stored as a raw Java object. For documents with many small fields, the per-field wrapper overhead adds up.

**No arbitrary Java types.** A HashMap can store any Java object. An ObjectNode can only store JSON-representable types. Lucille works around this (byte arrays are stored as base64-encoded binary nodes, Instants are stored as ISO-8601 strings), but the Document cannot natively hold arbitrary domain objects.

**Deep copy cost.** Copying a document requires `objectNode.deepCopy()`, which recursively copies the entire JSON tree. A HashMap with immutable String values would be cheaper to shallow-copy.

In practice, these tradeoffs are acceptable because pipeline stages typically read a small number of fields, do expensive work (API calls, model inference, text processing), and write a small number of fields. The per-access overhead is negligible relative to the actual enrichment work. The serialization savings at every boundary crossing more than compensate.

---

## Notable Design Choices in the API

### Uniform Single/Multi-Valued Access

The `getStringList()` method returns a `List<String>` regardless of whether the field is single-valued or multi-valued. If the field is single-valued, it wraps the value in a singleton list. This means a stage that processes "all values of a field" doesn't need to check whether the field is single or multi-valued first — it can always iterate over the list.

Conversely, `getString()` always returns the first value, whether the field is single or multi-valued. A stage that only cares about the primary value doesn't need to handle the list case.

### The setOrAdd Pattern

`setOrAdd()` is a single method that handles the most common field-writing pattern in search ingestion: "if this field doesn't exist yet, create it; if it does, append to it." Without this, every stage that accumulates values would need:

```java
if (doc.has("tags")) {
    doc.addToField("tags", newTag);
} else {
    doc.setField("tags", newTag);
}
```

With `setOrAdd`, it's one line: `doc.setOrAdd("tags", newTag)`. Across a pipeline with dozens of stages, this eliminates hundreds of lines of boilerplate.

### Typed Getters with Null Semantics

Every getter returns `null` for both "field absent" and "field present but null." The `has()` method distinguishes between these cases when it matters. This is a deliberate choice: most stage logic doesn't care *why* a value is missing — it just needs to handle the missing case. The rare stage that needs to distinguish "field not set" from "field explicitly null" can use `has()` + `hasNonNull()`.

### Reserved Fields with Triple-Underscore Prefix

Internal control fields (`___dropped`, `___skipped`, `___children`) use a triple-underscore prefix that is unlikely to appear in user data and is invalid in most search engine schemas. This ensures they never collide with user data fields and are automatically stripped by the Indexer before sending to the search backend. The `validateFieldNames()` method prevents stages from accidentally writing to reserved fields.

The triple-underscore prefix was chosen over the original dot prefix (`.dropped`, `.skipped`, `.children`) because the dot character is used as a path separator in nested JSON access (`getNestedJson("a.b.c")`). A reserved field named `.children` would be ambiguous — is it a top-level field named `.children` or a nested path? The `___` prefix eliminates this conflict entirely.

### Field Name Validation on Every Write

Every setter calls `validateFieldNames()` before writing. This catches two classes of bugs immediately:
1. Attempting to write to a reserved field (like `id` or `run_id`).
2. Passing a null or empty field name.

The validation happens at write time, not at indexing time, so bugs are caught in the stage that caused them rather than surfacing later in the pipeline.

### Insertion Order Preservation

`ObjectNode` uses a `LinkedHashMap` internally, so `getFieldNames()` returns fields in insertion order. This is a subtle but useful property: when a document is serialized to JSON, the fields appear in the order they were added. This makes debugging easier (the ID is always first, enrichment fields appear in pipeline order) and produces deterministic output for testing.

### JSONata Integration

The `transform()` method applies a JSONata expression directly to the Document's backing ObjectNode. JSONata is a query and transformation language for JSON — think of it as XPath/XSLT for JSON. Because the Document is already JSON, there's no conversion step. A stage can reshape a document's structure with a single expression:

```java
Jsonata expr = Jsonata.jsonata("{ 'fullName': firstName & ' ' & lastName }");
doc.transform(expr);
```

This is particularly powerful for stages that need to restructure complex nested data without writing procedural Java code.

### The asMap() Escape Hatch

`asMap()` converts the Document to a `Map<String, Object>` using Jackson's `MAPPER.convertValue()`. This is the escape hatch for code that needs a plain Map — typically when interfacing with libraries that expect Map input. It's deliberately not the primary API because it loses the typed access, the single/multi-valued distinction, and the zero-cost serialization. But it exists for interop.

---

## Summary

Lucille's Document API is designed around three principles:

1. **Match the search engine's field model** — single/multi-valued distinction, typed access, update modes that reflect how search fields are actually populated.
2. **Minimize serialization cost** — JSON backing means zero-cost boundary crossings in a system where documents cross many boundaries.
3. **Eliminate stage boilerplate** — `setOrAdd`, `update` with `UpdateMode`, uniform list access, and null handling reduce the per-stage code that isn't core logic.

The result is that a typical pipeline stage is a few lines of domain logic rather than a page of field-access ceremony. Across a pipeline with dozens of stages, this compounds into significantly less code, fewer bugs, and faster development.
