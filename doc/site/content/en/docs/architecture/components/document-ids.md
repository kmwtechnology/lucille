---
title: "Document IDs"
weight: 8
date: 2025-06-09
description: >
  Why IDs must be deterministic, how Lucille handles duplicates, and the idOverrideField mechanism.
---

## Why IDs Must Be Deterministic

In search ingestion, the document ID serves as the **primary key in the search index**. When you index a document with ID "ABC", the search engine either creates a new record or updates the existing record with that ID (upsert semantics). This has a critical implication:

**If you run the same ingest twice, you want the same documents to get the same IDs.** Otherwise, the second run creates duplicates instead of updating existing records. A search index with 100,000 documents would become 200,000 documents after a re-run — all duplicates.

Deterministic IDs mean:
- **Re-ingestion is safe.** Running the pipeline again updates existing documents rather than creating duplicates.
- **Incremental updates work.** A document that changes in the source system gets re-indexed under the same ID, replacing the old version.
- **Deletions are possible.** To delete a document from the index, you need to know its ID. If the ID was random, you'd have no way to reference it later.
- **Parent-child relationships are stable.** Child documents reference their parent by ID. If the parent's ID changes on re-run, the relationship breaks.

This is why Connectors are responsible for creating documents with meaningful, stable IDs derived from the source data — a file path, a database primary key, a URL — rather than random UUIDs.

---

## Why IDs Are Immutable in Lucille

Once a Document is created with an ID, that ID cannot be changed through the normal Document API. The `id` field is in the `RESERVED_FIELDS` set, so `setField("id", ...)`, `addToField("id", ...)`, and `renameField("id", ...)` all throw `IllegalArgumentException`.

This immutability exists because the ID is used as a **tracking key throughout the system**:

1. The **Publisher** registers the ID in its accounting ledger when the document is published. If the ID changed mid-pipeline, the Publisher would never receive a terminal event for the original ID — the run would hang.
2. The **Worker** sends CREATE events for child documents using their IDs. If a child's ID changed after the CREATE event was sent, the Publisher's accounting would be corrupted.
3. The **Kafka message key** is the document ID. Changing the ID mid-pipeline would break ordering guarantees (the document would land on a different partition after the change).
4. The **Indexer** uses the ID to send upserts and deletions to the search backend. The ID must be the same value that was tracked through the entire pipeline.

In short: the ID is the document's identity across all components. Mutating it would break accounting, ordering, and idempotency simultaneously.

---

## How Lucille Handles Duplicate IDs

Duplicate IDs — multiple documents with the same ID published in the same run — are a legitimate scenario. They arise in CDC (Change Data Capture) scenarios where a source system emits multiple updates for the same record, or when a connector reads from a source that contains duplicate entries.

Lucille handles duplicates at two levels:

### At the Publisher Level: The Bag Data Structure

The Publisher's `docIdsToTrack` is a `Bag<String>` (multiset), not a `Set<String>`. If two documents with ID "doc-1" are published, the Bag count for "doc-1" becomes 2. The Publisher expects to receive **two** separate terminal events for that ID. Each terminal event decrements the count by one. The run is not considered complete until the count reaches zero for all IDs.

```java
// Two documents with same ID published → bag count is 2
docIdsToTrack.add("doc-1");  // count: 1
docIdsToTrack.add("doc-1");  // count: 2

// First terminal event → count drops to 1
docIdsToTrack.remove("doc-1", 1);  // count: 1

// Second terminal event → count drops to 0
docIdsToTrack.remove("doc-1", 1);  // count: 0, now removed
```

This means duplicate IDs don't corrupt the accounting — each published document is tracked independently even if it shares an ID with another.

### At the Publisher Level: Collapsing Mode

For connectors that emit multiple consecutive documents with the same ID (common in CDC), the Publisher supports a **collapsing mode** (`requiresCollapsingPublisher() = true`). In this mode, consecutive same-ID documents are merged into a single document via `setOrAddAll()` before being sent for processing:

```java
if (previousDoc.getId().equals(document.getId())) {
    previousDoc.setOrAddAll(document);  // merge fields into one document
} else {
    sendForProcessing(previousDoc);     // different ID — send the previous one
    previousDoc = document;             // hold the new one
}
```

This reduces N consecutive same-ID documents to one document with multi-valued fields, which is then processed and indexed once. `numReceived` counts all N inputs; `numPublished` counts only the single merged output.

### At the Indexer Level: Upsert Semantics

When two documents with the same ID reach the Indexer (either because collapsing is not enabled, or because they were non-consecutive), the search engine's upsert semantics handle it: the second document overwrites the first in the index. The final state of the index reflects the last document indexed with that ID. Combined with Lucille's ordering guarantees (same ID → same partition → same consumer → sequential processing), this means the final state is deterministic.

### At the Indexer Level: Ordering Within a Batch

If a batch contains both an upsert and a delete for the same ID, the Indexer implementations (SolrIndexer, OpenSearchIndexer) explicitly handle ordering — flushing pending upserts before processing a delete for the same ID, or vice versa. This ensures that the operations are applied in the correct sequence even within a single batch.

---

## The idOverrideField: Decoupling Internal ID from Index ID

Sometimes the ID used internally for tracking is not the ID you want in the search index. For example:
- A Connector might use a composite key (source + record number) for tracking uniqueness, but the search index expects a simpler ID.
- A pipeline might compute a better ID during enrichment (e.g., hashing certain fields to create a deduplication key).
- A document might need different IDs in different destination indices.

Lucille solves this with `indexer.idOverrideField` — a configuration option that tells the Indexer to use a **different field's value** as the document's ID when sending to the search backend, without modifying the document's internal ID:

```hocon
indexer {
  idOverrideField: "computed_id"
}
```

### How It Works

1. The Connector creates the document with a stable internal ID (e.g., `"source-record-42"`)
2. A pipeline Stage computes a better ID and stores it in a field (e.g., `doc.setField("computed_id", "hash-abc123")`)
3. The document flows through the system tracked by its internal ID (`"source-record-42"`)
4. At indexing time, the Indexer calls `getDocIdOverride(doc)` which returns `"hash-abc123"`
5. The document is sent to the search backend with ID `"hash-abc123"`

### What Uses Which ID

The internal ID (`"source-record-42"`) is used for:
- Publisher accounting
- Kafka message key (ordering)
- Event tracking (CREATE, FINISH, FAIL)
- Logging and debugging

The override ID (`"hash-abc123"`) is used only for:
- The document's ID in the search index

This separation means the pipeline's internal correctness guarantees (ordering, accounting, fault tolerance) are never affected by what ID appears in the search index. The override is applied at the very last moment — after all tracking is complete — as a pure presentation concern.

---

---

## ID Generation Strategies: Good and Bad

### How Existing Connectors Generate IDs

**FileConnector: MD5 hash of the full file path.**

```java
String docId = DigestUtils.md5Hex(fullPath);
Document doc = Document.create(StorageClient.createDocId(docId, params));
```

The FileConnector uses the MD5 hash of the file's full URI (e.g., `s3://bucket/path/to/file.pdf`) as the document ID. This is a good strategy because:
- It's deterministic — the same file always produces the same ID
- It's stable across re-runs — re-ingesting the same file updates rather than duplicates
- It handles special characters — file paths with spaces, unicode, or long lengths are reduced to a fixed-length hex string that's safe for any search engine
- It's unique — different file paths produce different hashes (collision probability is negligible)

The `docIdPrefix` is then prepended via `StorageClient.createDocId()`, producing IDs like `"file-a1b2c3d4e5f6..."`.

**DatabaseConnector: Value from a configured ID column.**

```java
String id = createDocId(rs.getString(idColumn));
Document doc = Document.create(id);
```

The DatabaseConnector reads the ID from a column specified in config (`idField`). This is the natural choice for database sources because:
- The database already has a primary key that uniquely identifies each record
- It's deterministic and stable across re-runs
- It matches what the user expects — the search index ID corresponds to the database primary key

**ChunkText: Parent ID + chunk number.**

```java
String id = parentId + "-" + (i + 1);
Document childDoc = Document.create(id);
```

Child documents derive their IDs from the parent ID plus a positional suffix. This ensures:
- Children have unique IDs (parent ID is unique, suffix is unique within the parent)
- The relationship to the parent is visible in the ID itself
- Re-chunking the same parent produces the same child IDs (deterministic)

### Good ID Strategies

| Strategy | When to Use | Example |
|---|---|---|
| Database primary key | Source has a natural unique key | `"customer-42"`, `"order-10051"` |
| File path (or hash of it) | File-based sources | `md5("s3://bucket/file.pdf")` |
| URL | Web crawling | `md5("https://example.com/page")` |
| Composite key | Multiple fields needed for uniqueness | `"source-table-pk"` → `"crm-accounts-42"` |
| Parent ID + suffix | Child documents | `"doc-123-chunk-1"`, `"doc-123-chunk-2"` |
| Content hash | Deduplication across sources | `md5(title + body)` |

### Bad ID Strategies

| Strategy | Why It's Bad |
|---|---|
| `UUID.randomUUID()` | Not deterministic — re-running creates duplicates in the index |
| Auto-incrementing counter | Not stable — if source order changes, IDs shift; not unique across runs |
| Timestamp | Not unique if two documents are created in the same millisecond |
| Row number in result set | Changes if query order changes or rows are added/deleted |
| Mutable source field | If the field changes in the source, the document gets a new ID and the old one becomes an orphan in the index |

### When Random UUIDs Are Acceptable

There is one scenario where random UUIDs are acceptable: **when the index is always rebuilt from scratch** (full re-index, not incremental). If you drop and recreate the index on every run, duplicate IDs from re-runs are not a concern because the old index is gone. However, this limits you to full batch mode and prevents incremental updates.

### Hashing as an ID Strategy

Hashing (MD5, SHA-256) is useful when:
- The natural key is too long for a search engine ID field (some have length limits)
- The natural key contains characters that are problematic in URLs or APIs
- You want to deduplicate across sources (hash the content, not the source path)

The FileConnector's use of `DigestUtils.md5Hex(fullPath)` is a good example. The tradeoff is that the ID is opaque — you can't look at it and know which file it represents. The FileConnector mitigates this by also storing the full path in a `file_path` field on the document.

---

## The docIdPrefix: Namespacing IDs by Connector

When multiple connectors feed documents into the same index, their IDs might collide. A database connector and a file connector might both produce a document with ID "1". The `docIdPrefix` configuration on a connector prepends a string to all document IDs it creates:

```hocon
connectors: [
  {
    name: "database"
    class: "com.kmwllc.lucille.connector.DatabaseConnector"
    docIdPrefix: "db-"
    # produces IDs like "db-1", "db-2", ...
  },
  {
    name: "files"
    class: "com.kmwllc.lucille.connector.FileConnector"
    docIdPrefix: "file-"
    # produces IDs like "file-/path/to/doc.pdf", ...
  }
]
```

The prefix is applied by the connector when creating documents via `AbstractConnector.createDocId(id)`. It becomes part of the document's immutable ID from that point forward.

---

## Summary

| Concern | Mechanism |
|---|---|
| Deterministic IDs | Connector derives ID from source data |
| ID immutability | Reserved field protection in Document API |
| Duplicate ID accounting | Bag (multiset) in Publisher, not Set |
| Consecutive duplicate merging | Collapsing mode in Publisher |
| Non-consecutive duplicates | Search engine upsert semantics + ordering guarantees |
| Same-ID operations in one batch | Explicit ordering logic in Indexer implementations |
| Different ID in search index | `indexer.idOverrideField` |
| ID collision across connectors | `docIdPrefix` on each connector |
