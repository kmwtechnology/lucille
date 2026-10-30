---
title: CSV Indexer
weight: 4
date: 2025-06-09
description: Configuration reference for the CSV Indexer, which writes pipeline output to a CSV file.
---

`com.kmwllc.lucille.indexer.CSVIndexer`

Config block: `csv { ... }`

| Parameter | Type | Required | Description |
|---|---|---|---|
| `path` | String | Yes | Output CSV file path. The file is opened (and emptied, unless `append` is `true`) as soon as the indexer is created, before the rest of the config is validated. |
| `columns` | List\<String\> | Yes | Ordered list of document fields to write as columns. |
| `includeHeader` | Boolean | No | Write a header row of the column names when the indexer's connection is validated at startup. Default: `true`. |
| `append` | Boolean | No | Append to an existing file instead of overwriting it. Default: `false`. |

```hocon
indexer { type: "CSV" }
csv {
  path: "./output.csv"
  columns: ["id", "title", "body", "published_at"]
}
```

**Limitations:** CSVIndexer does not support `indexer.indexOverrideField` or `indexer.maxConcurrentBatches` greater than 1. If either is set, the indexer fails at startup. If the output file cannot be opened, an error is logged and the run stops at startup with "Indexer could not connect."

---

## Column Ordering and Field Selection

The `columns` list determines both which fields are written and their column order. Fields not listed in `columns` are silently omitted from the output. The document's `id` field is not automatically included. Add it to `columns` explicitly if you want it in the CSV. `indexer.whitelist`, `indexer.blacklist`, and `indexer.idOverrideField` are not applied, and child documents are not written.

A column whose field is missing from a document is written as an empty value. Other values, including empty strings, are written in double quotes.

---

## Multi-Valued Fields

When a document field is multi-valued, CSVIndexer writes the list's `toString()` representation (e.g., `[val1, val2]`). This is a lossy representation: the square brackets and commas become part of the string. If you need a different delimiter or format for multi-valued fields, flatten them in a pipeline stage before indexing.

---

## Append Mode

When `append: true`, the file is opened in append mode. The header row is still written unless `includeHeader` is `false`, so set `includeHeader: false` when appending to avoid duplicate header rows:

```hocon
csv {
  path: "./output.csv"
  columns: ["id", "title", "body"]
  append: true
  includeHeader: false
}
```

---

## Deletion Not Supported

CSVIndexer logs a warning if `deletionMarkerField` or `deleteByFieldField` are configured but does not perform any deletion. Documents marked for deletion are written as regular rows.

---

## sendEnabled

CSVIndexer does not skip writing when `indexer.sendEnabled` is `false` (bypass mode). The file is still created and rows are still written.

---

## Directory Creation

CSVIndexer creates parent directories automatically if they don't exist. You can specify a path like `./output/results/data.csv` without creating the directories first.

---

## Use Cases

CSVIndexer is primarily useful for:
- **Testing:** Verify pipeline output without a search backend.
- **Debugging:** Inspect what fields and values reach the indexer after pipeline processing.
- **Exporting:** Produce a file for import into another system.

It is not intended for production search indexing.
