---
title: NopIndexer (No-op)
weight: 5
date: 2025-06-09
description: A no-op indexer that discards all documents, useful for testing pipelines.
---

`com.kmwllc.lucille.indexer.NopIndexer`

Discards all documents without sending them anywhere. Useful for testing pipelines when indexing output is not needed. NopIndexer has no config block and no parameters beyond the shared `indexer { ... }` block.

NopIndexer is never selected by default: a config that selects no indexer fails at startup. To run with no destination, select it explicitly:

```hocon
indexer { type: "Nop" }
```

Documents still go through the normal indexer loop (batching, metrics, and completion events), so each document is reported as successfully indexed. Connection validation always passes. NopIndexer accepts `indexer.maxConcurrentBatches` greater than 1, so it can be used to exercise concurrent batching without a backend.

---

## When to Use

- **Testing pipeline logic:** Run your pipeline end-to-end without needing a search backend running.
- **Validating config:** Confirm that connectors, stages, and the indexer block are configured correctly before pointing at a real backend.
- **Benchmarking pipeline throughput:** Measure how fast your pipeline processes documents in isolation, without indexer latency as a factor.

---

## Comparison with sendEnabled: false

Setting `indexer.sendEnabled: false` starts the configured indexer in bypass mode. What bypass mode does is up to each indexer. For example, SolrIndexer creates no client and skips sending, while CSVIndexer still writes its file. NopIndexer discards documents regardless and does not need a backend-specific config block:

```hocon
# Option 1: NopIndexer
indexer {
  class: "com.kmwllc.lucille.indexer.NopIndexer"

  solr { url: "http://localhost:8983/solr" }
}

# Option 2: sendEnabled on a real indexer
indexer { type: "Solr", sendEnabled: false }
```

---

## Use in RunType.TEST

`Runner.runInTestMode()` does not use NopIndexer. Instead, it creates the configured indexer in bypass mode, the same as `sendEnabled: false`. This means your test config still needs a valid `indexer` block, and the result depends on how that indexer handles bypass mode. If you want to avoid configuring a backend entirely in test configs, use NopIndexer explicitly:

```hocon
indexer { class: "com.kmwllc.lucille.indexer.NopIndexer" }
```
