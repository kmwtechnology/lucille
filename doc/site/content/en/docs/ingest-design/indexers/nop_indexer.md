---
title: NopIndexer (No-op)
weight: 5
date: 2025-06-09
description: A no-op indexer that discards all documents — useful for testing pipelines.
---

`com.kmwllc.lucille.indexer.NopIndexer`

Discards all documents without sending them anywhere. Useful for testing pipelines when indexing output is not needed. Equivalent to setting `indexer.sendEnabled: false` on any other indexer.

```hocon
indexer { type: "Nop" }
```

---

## When to Use

- **Testing pipeline logic** — Run your pipeline end-to-end without needing a search backend running.
- **Validating config** — Confirm that connectors, stages, and the indexer block are configured correctly before pointing at a real backend.
- **Benchmarking pipeline throughput** — Measure how fast your pipeline processes documents in isolation, without indexer latency as a factor.

---

## Equivalence with sendEnabled: false

`NopIndexer` and `indexer.sendEnabled: false` on another indexer are functionally identical — both discard documents after the pipeline processes them. The difference is that NopIndexer doesn't require configuring a backend-specific block at all:

```hocon
# These are equivalent:

# Option 1: NopIndexer
indexer { type: "Nop" }

# Option 2: sendEnabled on a real indexer
indexer { type: "Solr", sendEnabled: false }
solr { url: "http://localhost:8983/solr/my-collection" }
```

---

## Use in RunType.TEST

`Runner.runInTestMode()` does not use NopIndexer. Instead, it creates the configured indexer with `bypass=true`, which causes `sendToIndex()` to skip the actual backend call while still running the full indexer loop (batching, events, metrics). This means your test config still needs a valid `indexer` block — but the backend does not need to be running. If you want to avoid configuring a backend block entirely in test configs, use NopIndexer explicitly:

```hocon
indexer { type: "Nop" }
```
