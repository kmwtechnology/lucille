---
title: Relevan Indexer
weight: 8
date: 2026-10-17
description: Configuration reference for the Relevan Indexer — index documents into a Relevan search index.
---

`com.kmwllc.lucille.relevan.indexer.RelevanIndexer`

Indexes documents into a [Relevan](https://relevan.dev) search index through the Relevan REST API. Config block: `relevan { ... }`

**Maven dependency:**
```xml
<dependency>
  <groupId>com.kmwllc</groupId>
  <artifactId>lucille-relevan</artifactId>
  <version>${lucille.version}</version>
</dependency>
```

| Parameter | Type | Required | Description |
|---|---|---|---|
| `apiKey` | String | Yes | An org-scoped Relevan API key. Use `${RELEVAN_API_KEY}` for environment variable substitution. |
| `index` | String | Yes | Name of the Relevan index to write to. |
| `orgId` | String | No | ID or slug of the Relevan organization. When omitted, it is resolved from the API key, which always belongs to exactly one organization. |
| `url` | String | No | Base URL of the Relevan API. Default: `"https://api.relevan.dev"`. |
| `dropUnmappedFields` | Boolean | No | Leave out fields that the index's mapping does not declare, instead of letting Relevan reject the document. Default: `false`. |
| `coerceTypes` | Boolean | No | Convert string values to numbers or booleans when the mapping declares the field with kind `number` or `boolean`. Default: `false`. |
| `requestTimeoutMs` | Integer | No | Timeout for connecting and for each request, in milliseconds. Default: `30000`. |

```hocon
indexer {
  class: "com.kmwllc.lucille.relevan.indexer.RelevanIndexer"
  maxRetries: 3
  deletionMarkerField: "is_deleted"
  deletionMarkerFieldValue: "true"
}

relevan {
  apiKey: ${RELEVAN_API_KEY}
  index: "songs"
}
```

---

## Before You Index: Create the Index and Its Mapping

The Relevan index must exist and have a mapping deployed before Lucille runs. Relevan rejects documents for an index without a mapping, and the indexer's connection check fails with a message saying so. Create the index in the [Relevan console](https://console.relevan.dev) or through the API:

```bash
curl -X POST https://api.relevan.dev/v1/{orgId}/indexes \
  -H "x-api-key: $RELEVAN_API_KEY" -H "content-type: application/json" \
  -d '{"name": "songs", "displayName": "Songs"}'

curl -X POST https://api.relevan.dev/v1/{orgId}/indexes/songs/mappings \
  -H "x-api-key: $RELEVAN_API_KEY" -H "content-type: application/json" \
  -d '{"fields": {
        "title":      {"use": ["searchable", "highlightable"]},
        "artist":     {"use": ["searchable", "filterable"], "kind": "text", "language": "none"},
        "top_genre":  {"use": ["searchable", "filterable", "aggregatable"], "kind": "text", "language": "none"},
        "added":      {"use": ["filterable", "sortable"], "kind": "date"},
        "popularity": {"use": ["filterable", "sortable"], "kind": "number"}
      }}'
```

`GET /v1/{orgId}/indexes/{index}/skill` returns a guide, generated from the live mapping, to choosing each field's `use` and `kind`.

**Include ranking signals.** Relevan tunes ranking with values that show which result is better, such as price, sales, popularity, stock, ratings or last-activity dates. Relevan can only use a signal that is a field on the document, so join these signals into your documents in the pipeline and map them as `number` or `date` fields that are `filterable` and `sortable`.

---

## Documents and Fields

- The Lucille document ID becomes the Relevan document ID. Set `indexer.idOverrideField` to use a different field.
- Lucille's `id` and `run_id` fields are never sent as content.
- Every other field is sent as content after `indexer.whitelist` and `indexer.blacklist` are applied, and Relevan rejects a document that has a field its mapping does not declare. Remove extra fields with the blacklist or a `DeleteFields` stage, or set `dropUnmappedFields: true`. This includes the deletion marker field on documents that are not deletions.
- Relevan does not accept arrays of objects, and does not search the contents of an object field. Flatten nested data into top-level fields in the pipeline.

### Document IDs

Relevan document IDs may only contain letters, digits, `_`, `.`, `:` and `-`, and may be up to 512 characters long. A document whose ID breaks these rules fails without being sent. File paths and URLs are common examples: use a stage to build a compliant ID field and point `indexer.idOverrideField` at it.

### Type Coercion

Relevan checks each value against the field's kind and does not convert strings: `"140"` is rejected for a `number` field. Sources such as CSV files produce only strings, so set `coerceTypes: true` to convert values for `number` and `boolean` fields, including each value in a list. A string that does not parse, such as an empty string, is sent unchanged and Relevan rejects the document. Dates are sent as they are: Lucille stores dates as ISO-8601 strings, which Relevan accepts.

`dropUnmappedFields` and `coerceTypes` read the index's latest mapping once, the first time the indexer writes to the index. Restart Lucille after deploying a new mapping revision.

---

## Batching and Validation Errors

Upserts are sent through Relevan's bulk endpoint, up to 1000 documents per request. A larger `indexer.batchSize` is split into several requests.

Relevan rejects a whole bulk request when any document in it is not valid, and reports every failing document. The indexer fails only those documents, with Relevan's message for each, and sends the rest again, so one bad document does not fail its batch.

When the same document ID appears more than once in a batch, the operations on it are sent in order in separate requests.

---

## Deletion

Deletion is by ID only, using `deletionMarkerField` / `deletionMarkerFieldValue`. Deleting a document that does not exist counts as success. Delete-by-query (`deleteByFieldField` / `deleteByFieldValue`) is not supported: the indexer refuses to start when it is configured.

---

## Retries

HTTP 429 (rate limit) and 5xx responses and network errors are retryable. Set `indexer.maxRetries` to retry them with exponential backoff. Other failures are not retried: HTTP 402 (`storage_limit_exceeded`) means the organization's storage quota is full, and 401 means the API key is invalid.

---

## Example: CSV to Relevan

This pipeline indexes the `songs.csv` file from the `lucille-simple-csv-solr-example` into the `songs` index above. Relevan field names cannot contain spaces, so the CSV columns are renamed. `coerceTypes` turns the CSV's numeric strings into numbers, and `dropUnmappedFields` drops the columns the mapping leaves out. The `added` column needs its dashes replaced before Relevan accepts it as a date.

```hocon
connectors: [
  {
    class: "com.kmwllc.lucille.connector.FileConnector"
    paths: ["conf/songs.csv"]
    name: "songs"
    pipeline: "songs"
    fileHandlers: { csv: { } }
  }
]

pipelines: [
  {
    name: "songs"
    stages: [
      {
        class: "com.kmwllc.lucille.stage.RenameFields"
        fieldMapping: { "top genre": "top_genre", "pop": "popularity" }
      }
      {
        # songs.csv writes its dates with U+2011 non-breaking hyphens, which Relevan does not accept in a date
        class: "com.kmwllc.lucille.stage.ReplacePatterns"
        source: ["added"]
        dest: ["added"]
        regex: ["‑"]
        replacement: "-"
      }
    ]
  }
]

indexer {
  class: "com.kmwllc.lucille.relevan.indexer.RelevanIndexer"
  maxRetries: 3
}

relevan {
  apiKey: ${RELEVAN_API_KEY}
  index: "songs"
  dropUnmappedFields: true
  coerceTypes: true
}
```

---

## Troubleshooting

**"Relevan index '...' has no mapping":**
Deploy a mapping to the index before indexing. See [Before You Index](#before-you-index-create-the-index-and-its-mapping).

**"Unknown field '...' is not declared in the index mapping":**
The document has a field the mapping does not declare. Add the field to the mapping, remove it in the pipeline, or set `dropUnmappedFields: true`.

**"Field '...' must be of kind 'number'":**
The value is a string or does not parse as a number. Set `coerceTypes: true`, or convert the value in the pipeline.

**"is not a valid Relevan document ID":**
See [Document IDs](#document-ids).

**HTTP 401:**
Verify `RELEVAN_API_KEY` is set and the key belongs to the organization that owns the index.
