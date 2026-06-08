---
title: "Quick Reference"
weight: 40
date: 2025-06-09
description: >
  Concise code references for the patterns developers use most frequently — threading, Document API, child documents, conditions, and common mistakes.
---

This page provides copy-paste-ready examples for the patterns developers use most frequently. It complements the detailed explanations in the Architecture and Contributing sections.

---

## The Threading Model

**Each Worker thread creates its own Pipeline object, which creates its own instance of every Stage.** Instance fields in a Stage are effectively thread-local — use them freely without synchronization.

**When to use a singleton:** If a resource is both expensive to initialize *and* thread-safe for concurrent use, share it via a static field with lazy initialization. The existing `DictionaryManager` class is the canonical pattern.

```java
// Per-thread resource (typical case — no synchronization needed)
private Connection dbConnection;

@Override
public void start() throws StageException {
  this.dbConnection = openConnection();  // one per thread, safe
}

// Shared resource (for expensive, thread-safe resources only)
private static volatile MyExpensiveModel sharedModel;
private static final Object modelLock = new Object();

@Override
public void start() throws StageException {
  if (sharedModel == null) {
    synchronized (modelLock) {
      if (sharedModel == null) {
        sharedModel = loadModel();
      }
    }
  }
}
```

Do not default to singletons. Most stages should use per-thread instance fields.

---

## Reading Configuration

```java
// Required parameters — throw if absent
String source = config.getString("source");
int batchSize = config.getInt("batchSize");
List<String> fields = config.getStringList("fields");

// Optional parameters — use ConfigUtils.getOrDefault
String dest = ConfigUtils.getOrDefault(config, "dest", "output");
int limit = ConfigUtils.getOrDefault(config, "limit", 100);
boolean nested = ConfigUtils.getOrDefault(config, "isNested", false);

// Check presence before reading
if (config.hasPath("optionalBlock")) {
  Config sub = config.getConfig("optionalBlock");
}

// UpdateMode (a common Lucille pattern)
UpdateMode mode = UpdateMode.fromConfig(config);
```

---

## The Document API

```java
// Create
Document doc = Document.create("my-unique-id");

// Set a field (overwrites any existing value)
doc.setField("title", "Hello World");

// Add to a field (creates multi-valued field)
doc.addToField("tags", "search");
doc.addToField("tags", "etl");

// setOrAdd: creates single-valued if absent, appends if present
doc.setOrAdd("tags", "lucille");

// update with UpdateMode
doc.update("title", UpdateMode.OVERWRITE, "New Title");
doc.update("tags", UpdateMode.APPEND, "newTag1", "newTag2");
doc.update("title", UpdateMode.SKIP, "Ignored");  // skips if already set

// Getters
String title = doc.getString("title");
List<String> tags = doc.getStringList("tags");
Boolean flag = doc.getBoolean("active");
Integer count = doc.getInt("count");
Instant ts = doc.getInstant("created_at");

// Presence check
if (doc.has("optionalField")) { ... }

// Drop a document (will not be sent to the indexer)
doc.setField(Document.DROP_FIELD, true);

// Reserved field names
// Document.ID_FIELD = "id"
// Document.RUNID_FIELD = "run_id"
// Document.CHILDREN_FIELD = "___children"
// Document.DROP_FIELD = "___dropped"
// Document.SKIP_FIELD = "___skipped"
```

**Supported types:** `String`, `Boolean`, `Integer`, `Double`, `Float`, `Long`, `Instant`, `byte[]`, `JsonNode`, `Timestamp`, `Date`.

**Nested JSON:** Documents support reading and writing into nested JSON structures using dot-and-bracket path syntax:

```java
// Get a nested value
JsonNode node = doc.getNestedJson("a.b[2].c");

// Set a nested value
doc.setNestedJson("a.b[2].c", jsonNode);

// Remove a nested value
doc.removeNestedJson("a.b[2].c");

// Parse and stringify segment paths
List<Document.Segment> segments = Document.Segment.parse("a.b[2].c");
String path = Document.Segment.stringify(segments);
```

---

## Child Documents

For the full reference on all control flow options — conditions, skipping, dropping, error handling, child documents, connector sequencing, and pre/post-connector actions — see [Control Flow]({{< relref "docs/reference/control-flow" >}}) in the Ingest Designer Guide.

Lucille has two distinct concepts of child documents: **attached** and **emitted**.

**Attached children** are stored inside a parent via `doc.addChild(childDoc)`. They travel with the parent and are not independently tracked or indexed.

**Emitted children** are returned from `processDocument()` as an Iterator. They become independent documents flowing through downstream stages, tracked by the Publisher, and indexed as separate records.

The bridge: `EmitNestedChildren` converts attached children to emitted children.

### Attaching children

```java
// Inside processDocument(): attach children to the parent (they travel with it)
Document child = Document.create(doc.getId() + "-chunk-1");
child.setField("text", chunkText);
doc.addChild(child);
return null;  // no documents emitted into the pipeline
```

### Emitting children

```java
// Inside processDocument(): emit children as independent pipeline documents
@Override
public Iterator<Document> processDocument(Document doc) throws StageException {
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

### Converting attached to emitted (the ChunkText + EmitNestedChildren pattern)

```hocon
stages: [
  {
    class: "com.kmwllc.lucille.stage.ChunkText"
    source: "body"
    chunkingMethod: "paragraph"
  },
  {
    class: "com.kmwllc.lucille.stage.EmitNestedChildren"
    dropParent: true
    fieldsToCopy: { "title": "parent_title" }
  }
]
```

---

## Conditions on Stages

Every Stage automatically supports a `conditions` configuration block:

```hocon
{
  class: "com.kmwllc.lucille.stage.OpenAIEmbed"
  source: "text"
  conditions: [
    { fields: ["text"], operator: "must" }
    { fields: ["type"], values: ["article"], operator: "must" }
  ]
  conditionPolicy: "all"
}
```

Do not write conditional logic inside `processDocument` for cases where you simply want to skip documents missing a field — use the `conditions` block instead.

---

## Plugin vs. Core

**Add to `lucille-core`** if the component has no heavy dependencies and is general-purpose.

**Create a new Maven module under `lucille-plugins/`** if the component depends on a large library, would cause transitive conflicts, or is specialized. Follow the structure of an existing plugin as a template.

---

## Common Mistakes to Avoid

1. **Missing or non-public SPEC.** Must be `public static final Spec SPEC`. Causes RuntimeException at startup, not a compile error.
2. **Reading config properties not declared in the SPEC.** The SPEC validates that no unrecognized properties are present. If you read `config.getString("myParam")` but didn't declare it in the SPEC, config validation will reject any pipeline that tries to set it.
3. **Using static mutable fields without synchronization.** Instance fields are safe (per-thread instantiation); static fields are shared across all threads.
4. **Calling `doc.getString()` on a multi-valued field.** Use `doc.getStringList()` when a field may be multi-valued.
5. **Putting cleanup logic in `postExecute`.** `postExecute` is NOT called after a failed `execute`. Put always-run cleanup in `close()`.
6. **Emitting children without `EmitNestedChildren`.** `ChunkText` attaches children to the parent; they are not indexed independently unless `EmitNestedChildren` follows.
7. **Using JSON/YAML syntax in HOCON.** HOCON style omits quotes around keys and uses `:` for assignment.
8. **Omitting Javadoc on new Stages.** All existing stages document their config parameters in Javadoc. Follow this convention.

---
---
