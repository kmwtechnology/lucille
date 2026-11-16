---
weight: 10
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
  -Dlog4j2.configurationFile=classpath:lucille-log4j2.xml \
  -Dconfig.file=/path/to/config.conf \
  -cp 'lucille-all/target/lib/*' \
  com.kmwllc.lucille.core.Runner
```

**Distributed mode:**

```bash
java \
  -Dlog4j2.configurationFile=classpath:lucille-log4j2.xml \
  -Dconfig.file=/path/to/config.conf \
  -cp 'lucille-all/target/lib/*' \
  com.kmwllc.lucille.core.Runner \
  -distributed
```

**External mode** (single JVM, Kafka messaging):

```bash
java \
  -Dlog4j2.configurationFile=classpath:lucille-log4j2.xml \
  -Dconfig.file=/path/to/config.conf \
  -cp 'lucille-all/target/lib/*' \
  com.kmwllc.lucille.core.Runner \
  -external
```

**Config validation only (no run):**

Validates every Connector, Stage, and Indexer spec and prints all errors. Exits without executing anything.

```bash
java \
  -Dlog4j2.configurationFile=classpath:lucille-log4j2.xml \
  -Dconfig.file=/path/to/config.conf \
  -cp 'lucille-all/target/lib/*' \
  com.kmwllc.lucille.core.Runner \
  -validate
```

**Render effective config (no run):**

Prints the fully resolved configuration after HOCON substitutions (environment variables, `include` directives, etc.). Useful for debugging config variable expansion.

```bash
java \
  -Dlog4j2.configurationFile=classpath:lucille-log4j2.xml \
  -Dconfig.file=/path/to/config.conf \
  -cp 'lucille-all/target/lib/*' \
  com.kmwllc.lucille.core.Runner \
  -render
```

## Run Configuration

```hocon
runner {
  # Level for the stage-by-stage metrics logged at end of run: TRACE, DEBUG, INFO, WARN or ERROR (default: DEBUG)
  metricsLoggingLevel: "INFO"

  # Connector timeout in milliseconds (default: 86400000 = 24 hours; set <= 0 to disable)
  connectorTimeoutMs: 86400000
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

The Runner registers a JVM shutdown hook, so `SIGINT` (Ctrl+C) and `SIGTERM` stop the current run as follows:
1. The Connector and the Publisher are closed, so no more documents are published.
2. The Worker pool is stopped. Each Worker finishes the document it is working on, and the Runner waits up to 3 seconds for them.
3. The Indexer is stopped. It tries to send its current batch, and the Runner waits up to 3 seconds for it.

In a local run, documents still waiting on the in-memory queues are not processed, and no run summary is logged. The process exits with the JVM's signal status: 130 for `SIGINT`, 143 for `SIGTERM`.

## RunType

Lucille supports four run types, selected via command-line flags:

| RunType | Flag(s) | Description |
|---|---|---|
| `LOCAL` | (none) | Single JVM, in-memory queues. Default. |
| `EXTERNAL` | `-external` | Single JVM, Kafka messaging. |
| `DISTRIBUTED` | `-distributed` | Separate JVMs per component, Kafka messaging. |
| `TEST` | (API only) | Single JVM, in-memory, search backend bypassed, messages captured. |

---

## Practical Guide

For deployment instructions — starting runs in each mode, command-line flags, and operational considerations — see [Deployment]({{< relref "docs/operations/deployment" >}}).

For runner and other top-level configuration parameters, see [Writing a Config]({{< relref "docs/ingest-design/writing-a-config" >}}).
