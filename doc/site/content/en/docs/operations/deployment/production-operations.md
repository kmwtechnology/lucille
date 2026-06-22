---
title: Production Operations
weight: 7
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

## Exit Codes

The Runner process exits with code **0** on success and code **1** on failure. In containerized environments, this exit code determines whether the container is considered to have failed — which in turn drives the Kubernetes container restart policy (`restartPolicy`) and Job retry behavior (`backoffLimit`).

Put simply: exit 0 means "the work was attempted for every document" while exit 1 means "the work was not attempted or could not be completed for all documents." A run with 1,000 document failures out of 1,000,000 is exit 0. A run where the database was unreachable from the start is exit 1.

### What produces exit 0 (success)

The run *completed*. All configured connectors executed to completion, documents flowed through their respective pipelines, and each document reached a terminal state. Some individual documents may have failed along the way (bad data, mapping mismatches, stage exceptions), but the system itself was healthy and did its job. The run is done and doesn't need to be retried as a whole, though an administrator may want to follow up on individual document failures.

### What produces exit 1 (failure)

The run *couldn't complete*. Something at the infrastructure level went wrong: a connector couldn't reach its source system, the indexer couldn't connect to the search backend, the configuration was invalid, or an unexpected framework-level error interrupted execution. Retrying may help (e.g. if the source system was temporarily unreachable), or human intervention may be needed (e.g. if the config is malformed).

The specific conditions that produce exit 1:

| Category | Specific condition |
|---|---|
| **CLI** | Unrecognized or missing command-line options |
| **Validation** | Pre-run config validation fails (invalid connector, pipeline, indexer, or other config) |
| **Worker startup** | Worker pool fails to start for a pipeline |
| **Indexer** | Indexer cannot be created from config, or fails to validate its connection to the search backend |
| **Connector lifecycle** | `preExecute()`, `execute()`, or `postExecute()` throws a `ConnectorException` |
| **Publisher** | `waitForCompletion()` throws an exception |
| **Connector timeout** | Connector exceeds `runner.connectorTimeout` (default: 24 hours) without completing |
| **Connector thread exception** | The connector thread throws an unhandled exception during execution |
| **Resource cleanup** | Error closing the connector or publisher after execution |

When any connector fails, subsequent connectors are skipped and the run exits 1 immediately.

### Key nuance for container orchestration

Individual document failures do **not** produce a non-zero exit code. This means:

- A Kubernetes CronJob with `restartPolicy: OnFailure` will retry only for infrastructure failures, not for document-level problems.
- If you need to treat document failures as a container failure (e.g. to trigger alerts or retries), check the run summary in logs rather than relying on the exit code. Alternatively, wrap the Runner invocation in a script that parses the summary and exits non-zero when `docs FAILED` exceeds a threshold.

### Entrypoint scripts and pipefail

When the Runner is the last command in a Docker entrypoint (or in a script with `set -eo pipefail`), its exit code becomes the container's exit code directly:

```dockerfile
ENTRYPOINT ["sh", "-c", "java -Xmx4g -Dconfig.file=${CONF} -cp 'lib/*' com.kmwllc.lucille.core.Runner"]
```

If you pipe Runner output through another command (e.g. `tee`), ensure `pipefail` is set so a Runner failure isn't masked:

```bash
#!/bin/bash
set -eo pipefail
java -Xmx4g -Dconfig.file="${CONF}" -cp 'lib/*' com.kmwllc.lucille.core.Runner 2>&1 | tee /var/log/lucille.log
```

### Graceful shutdown exit code

When the Runner receives `SIGINT` (e.g. from Kubernetes sending `SIGTERM` → the JVM's shutdown hook), it attempts a clean shutdown of all components and exits **0**. This means a pod terminated by Kubernetes during a scale-down or rolling update is not treated as a failure.

## Production Checklist

- [ ] Set `-Xmx` heap limit appropriate for your pipeline's memory usage.
- [ ] Configure `publisher.queueCapacity` (local mode) or `publisher.maxPendingDocs` (distributed).
- [ ] Tune `indexer.batchSize` and `indexer.batchTimeout` for your backend's throughput.
- [ ] Use environment variable substitution for credentials (`${?VAR_NAME}`) — never hard-code secrets in config files.
- [ ] Route `DocLogger` output to a separate file in production (see [Logging]({{< relref "docs/operations/logging" >}})).
- [ ] Enable `runner.metricsLoggingLevel: "INFO"` for stage-by-stage metrics at run completion.
- [ ] Configure `worker.maxRetries` and ZooKeeper if poison-pill protection is needed — without this, a document that repeatedly crashes a Worker will stall the pipeline indefinitely. Failed documents are routed to the `{pipeline}_fail` topic for inspection and replay.
- [ ] Set `runner.connectorTimeout` if any connector might run longer than 24 hours (the default timeout).
