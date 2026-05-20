---
title: Production Operations
weight: 5
date: 2025-06-09
description: Memory sizing, backpressure, batch tuning, graceful shutdown, monitoring, and the production checklist.
---

## Memory Sizing

Rule of thumb: `worker.threads × (Stage resource usage)`. A pipeline that loads a 500MB NLP model uses 500MB × number of Worker threads.

| Component | Typical Heap Size |
|---|---|
| Runner (local, lightweight pipeline) | 512MB–2GB |
| Runner (local, ML stages) | 4GB–16GB per stage model × threads |
| Worker (distributed, no ML) | 512MB–1GB |
| Worker (distributed, with ML models) | 2GB–8GB per model |
| Indexer | 512MB–1GB |

Always set `-Xmx` explicitly. Avoid letting Java use all available RAM.

## Backpressure and Queue Sizing

In local mode, set `publisher.queueCapacity` to bound the number of in-flight documents:

```hocon
publisher {
  queueCapacity: 10000
}
```

In distributed mode, use `publisher.maxPendingDocs` to throttle the Connector:

```hocon
publisher {
  maxPendingDocs: 80000
}
```

Without backpressure, a fast Connector can publish faster than Workers process, causing out-of-memory conditions.

## Indexer Batch Tuning

The Indexer sends documents in batches. Both batch size and timeout are independently configurable:

```hocon
indexer {
  batchSize: 200        # Flush when this many docs accumulate
  batchTimeout: 5000    # Flush after 5 seconds if batchSize is not reached
}
```

Larger batches improve indexing throughput (fewer API calls). The timeout prevents documents from waiting indefinitely when volume is low.

## Graceful Shutdown

Lucille handles `SIGINT` (Ctrl+C) and `SIGTERM` gracefully. On receiving a signal:
1. The Connector stops publishing new documents.
2. Workers drain the remaining documents.
3. The Indexer flushes its current batch.
4. The process exits with a run summary.

Do not send `SIGKILL` — documents in flight may not be indexed.

## Monitoring

See [Logging]({{< relref "docs/operations/logging" >}}) for setting up log monitoring.

Key metrics logged periodically during a run:

```
INFO PublisherImpl: 37029 docs published. One minute rate: 3225.69 docs/sec. Waiting on 21014 docs.
INFO WorkerPool: 27017 docs processed. One minute rate: 1787.10 docs/sec. Mean pipeline latency: 10.63 ms/doc.
INFO Indexer: 17016 docs indexed. One minute rate: 455.07 docs/sec. Mean backend latency: 6.90 ms/doc.
```

The frequency is controlled by `log.seconds` (default: 30).

## Run Summary

At the end of every run, Lucille prints a structured summary:

```
RUN SUMMARY: Success. 1/1 connectors complete. All published docs succeeded.
connector1: complete. 200000 docs succeeded. 0 docs failed. 0 docs dropped. Time: 416.47 secs.
Run took 417.46 secs.
```

A connector that failed entirely is distinguished from one that completed with individual document failures. Subsequent connectors after a failure are listed as skipped.

## Production Checklist

- [ ] Set `-Xmx` heap limit appropriate for your pipeline's memory usage.
- [ ] Configure `publisher.queueCapacity` (local mode) or `publisher.maxPendingDocs` (distributed).
- [ ] Tune `indexer.batchSize` and `indexer.batchTimeout` for your backend's throughput.
- [ ] Use environment variable substitution for credentials (`${?VAR_NAME}`) — never hard-code secrets in config files.
- [ ] Route `DocLogger` output to a separate file in production (see [Logging]({{< relref "docs/operations/logging" >}})).
- [ ] Enable `runner.metricsLoggingLevel: "INFO"` for stage-by-stage metrics at run completion.
- [ ] Configure `worker.maxRetries` and ZooKeeper if poison-pill protection is needed — without this, a document that repeatedly crashes a Worker will stall the pipeline indefinitely. Failed documents are routed to the `{pipeline}_fail` topic for inspection and replay.
- [ ] Set `runner.connectorTimeout` if any connector might run longer than 24 hours (the default timeout).
