---
title: Runner
date: 2025-10-23
description: Component that manages a Lucille Run end-to-end.
---

The *Runner* is the command-line entry point for launching a Lucille run. When invoked, it reads the configuration file, validates all component configurations, generates a unique `runId`, launches the configured components, waits for all work to complete, and prints a run summary.

## What a Run Is

A Lucille **Run** is a sequence of Connectors executed one after the other. Each Connector feeds a specific Pipeline. A run can include multiple Connectors feeding multiple Pipelines, all sharing the same Indexer.

Connectors run strictly in sequence: the next Connector does not start until all documents from the previous Connector have been fully processed and indexed. This ordering guarantee is enforced automatically by the Publisher's accounting system.

## Run Lifecycle

For each Connector in the configured sequence, the Runner:

1. Validates the full configuration (fails fast on any misconfiguration).
2. Starts a `WorkerPool` (N Worker threads based on `worker.threads`).
3. Starts an Indexer thread.
4. Creates a `PublisherImpl` and launches the Connector in a `ConnectorThread`.
5. Blocks on `publisher.waitForCompletion()` until all work is done.
6. Logs the run summary and moves to the next Connector (or exits).

## Starting a Run

**Local mode (default):**

```bash
java \
  -Dconfig.file=/path/to/config.conf \
  -cp 'lucille-core/target/lucille.jar:lucille-core/target/lib/*' \
  com.kmwllc.lucille.core.Runner
```

**Distributed mode (Kafka):**

```bash
java \
  -Dconfig.file=/path/to/config.conf \
  -cp 'lucille-core/target/lucille.jar:lucille-core/target/lib/*' \
  com.kmwllc.lucille.core.Runner \
  -useKafka
```

**Config validation only (no run):**

Validates every Connector, Stage, and Indexer spec and prints all errors. Exits without executing anything.

```bash
java \
  -Dconfig.file=/path/to/config.conf \
  -cp 'lucille-core/target/lucille.jar:lucille-core/target/lib/*' \
  com.kmwllc.lucille.core.Runner \
  -validate
```

**Render effective config (no run):**

Prints the fully resolved configuration after HOCON substitutions (environment variables, `include` directives, etc.). Useful for debugging config variable expansion.

```bash
java \
  -Dconfig.file=/path/to/config.conf \
  -cp 'lucille-core/target/lucille.jar:lucille-core/target/lib/*' \
  com.kmwllc.lucille.core.Runner \
  -render
```

## Run Configuration

```hocon
runner {
  # Log detailed stage-by-stage metrics at end of run (default: INFO)
  metricsLoggingLevel: "INFO"

  # Connector timeout in milliseconds (default: 86400000 = 24 hours; set <= 0 to disable)
  connectorTimeout: 86400000
}
```

## Run ID

The Runner generates a UUID `runId` for each run. The run ID is:
- Stamped on every Document by the Publisher (`run_id` field).
- Used as part of the Kafka event topic name in distributed mode.
- Included in log MDC so all log lines during a run include the run ID for filtering.

## Run Summary

At the end of every run, the Runner logs a structured summary:

```
RUN SUMMARY: Success. 1/1 connectors complete. All published docs succeeded.
connector1: complete. 200000 docs succeeded. 0 docs failed. 0 docs dropped. Time: 416.47 secs.
Run took 417.46 secs.
```

A connector that failed entirely is distinguished from one that completed with individual document failures. Connectors after a failed one are listed as skipped.

## Graceful Shutdown

The Runner handles `SIGINT` (Ctrl+C) and `SIGTERM`. On signal receipt:
1. The Connector stops publishing.
2. Workers drain remaining documents.
3. The Indexer flushes its current batch.
4. A partial run summary is logged.

## RunType

Lucille supports four run types, selected via command-line flags:

| RunType | Description |
|---|---|
| `LOCAL` | Single JVM, in-memory queues. Default. |
| `KAFKA_LOCAL` | Single JVM, Kafka messaging. |
| `KAFKA_DISTRIBUTED` | Separate JVMs per component, Kafka messaging. |
| `TEST` | Single JVM, in-memory, search backend bypassed, messages captured. |
