---
title: Runner Orchestration
weight: 1
date: 2025-06-09
description: >
  How Runner.run() coordinates the full lifecycle — validation, connector loop, signal handling, and reporting.
---

## Overview

The `Runner` is Lucille's top-level orchestrator. It coordinates the full lifecycle of a run: validating configuration, instantiating components, executing connectors sequentially, and reporting results. All methods are static — the Runner is never instantiated.

A "run" is a sequential execution of one or more Connectors. Each Connector's work must complete before the next begins. If any Connector fails, the run aborts.

## How `Runner.run()` Coordinates the Full Lifecycle

The main execution flow:

```java
public static RunResult run(Config config, RunType type, String runId) throws Exception {
    if (runId == null) {
        runId = Runner.generateRunId();  // UUID
    }
    MDC.put(RUNID_FIELD, runId);

    // 1. Validate FIRST
    Map<String, List<Exception>> validationErrors = runInValidationMode(config);
    if (!validationErrors.isEmpty()) {
        return new RunResult(false, ...);
    }

    // 2. Create connectors
    List<Connector> connectors = Connector.fromConfig(config);

    // 3. Execute each connector sequentially
    for (Connector connector : connectors) {
        // Create messenger factories based on RunType
        // Run connector with components
        ConnectorResult result = runConnectorWithComponents(...);
        if (!result.getStatus()) {
            return new RunResult(false, ...);  // Abort on failure
        }
    }

    return new RunResult(true, ...);
}
```

## The Validation Step

Before any work begins, the Runner validates the entire configuration:

```java
Map<String, List<Exception>> validationErrors = runInValidationMode(config);
```

This validates:
- **Pipelines** — each pipeline's stages are instantiated to check for config errors
- **Connectors** — each connector's config is checked for required/optional properties
- **Indexer** — the indexer config block is validated
- **Other parents** — publisher, runner, kafka, and other top-level config blocks

Validation is fail-all, not fail-fast. All errors are collected and reported together. If any validation errors exist, the run returns immediately with a failure result.

## The Connector Loop

For each connector, the Runner:

1. **Creates messenger factories** appropriate for the RunType
2. **Calls `runConnectorWithComponents()`** which:
   - Starts a `WorkerPool` (if local mode)
   - Creates and starts an `Indexer` thread (if local mode)
   - Creates a `Publisher`
   - Calls `runConnector()` which:
     - Calls `connector.preExecute(runId)`
     - Launches a `ConnectorThread` that calls `connector.execute(publisher)` then `publisher.flush()`
     - Calls `publisher.waitForCompletion(connectorThread, timeout)`
     - Calls `connector.postExecute(runId)` (only if publishing succeeded)
3. **Stops WorkerPool and Indexer** in the `finally` block

```java
private static ConnectorResult runConnectorWithComponents(...) {
    try {
        if (startWorkerAndIndexer && connector.getPipelineName() != null) {
            workerPool = new WorkerPool(config, pipelineName, localRunId, workerMessengerFactory, metricsPrefix);
            workerPool.start();

            IndexerMessenger indexerMessenger = indexerMessengerFactory.create();
            indexer = IndexerFactory.fromConfig(config, indexerMessenger, bypassIndexer, metricsPrefix, localRunId);
            indexerThread = new Thread(indexer);
            indexerThread.start();
        }

        publisher = new PublisherImpl(config, publisherMessenger, runId, ...);
        return runConnector(config, runId, connector, publisher);
    } finally {
        if (workerPool != null) { workerPool.stop(); workerPool.join(3000); }
        if (indexerThread != null) { indexer.terminate(); indexerThread.join(3000); }
    }
}
```

## How RunType Affects Component Startup

```java
public enum RunType {
    LOCAL,             // Workers + Indexer as threads; in-memory queues
    TEST,              // Same as LOCAL but bypass indexer backend; record message history
    KAFKA_LOCAL,       // Workers + Indexer as threads; Kafka for messaging
    KAFKA_DISTRIBUTED  // No local Workers/Indexer; Kafka messaging; assume external processes
}
```

| RunType | Start Worker/Indexer? | Bypass Indexer Backend? | Messenger Type |
|---------|----------------------|------------------------|----------------|
| `LOCAL` | Yes | No | `LocalMessenger` |
| `TEST` | Yes | Yes | `TestMessenger` |
| `KAFKA_LOCAL` | Yes | No | `KafkaWorkerMessenger` / `KafkaIndexerMessenger` |
| `KAFKA_DISTRIBUTED` | No | No | `KafkaWorkerMessenger` / `KafkaIndexerMessenger` |

The key decision:
```java
boolean startWorkerAndIndexer = !type.equals(RunType.KAFKA_DISTRIBUTED);
boolean bypassSolr = type.equals(RunType.TEST);
```

## The MessengerFactory Pattern

The Runner uses factory interfaces to decouple component creation from the RunType decision:

```java
if (RunType.TEST.equals(type)) {
    TestMessenger messenger = new TestMessenger();
    history.put(connector.getName(), messenger);
    workerMessengerFactory = WorkerMessengerFactory.getConstantFactory(messenger);
    indexerMessengerFactory = IndexerMessengerFactory.getConstantFactory(messenger);
    publisherMessengerFactory = PublisherMessengerFactory.getConstantFactory(messenger);
} else if (RunType.LOCAL.equals(type)) {
    LocalMessenger messenger = new LocalMessenger(config);
    workerMessengerFactory = WorkerMessengerFactory.getConstantFactory(messenger);
    // ...
} else {
    workerMessengerFactory = WorkerMessengerFactory.getKafkaFactory(config, connector.getPipelineName());
    // ...
}
```

For LOCAL/TEST modes, a single messenger instance is shared (via `getConstantFactory`). For Kafka modes, each factory call creates a new messenger with its own Kafka consumer/producer.

## Signal Handling for Clean Shutdown

When running via `main()`, the Runner registers an INT signal handler:

```java
state = new RunnerState();

Signal.handle(new Signal("INT"), signal -> {
    if (state != null) {
        state.close();  // Close connector, publisher, workerPool, indexer
    }
    SystemHelper.exit(0);
});
```

`RunnerState` holds references to the currently-active components. When a connector starts, the state is populated:

```java
if (state != null) {
    state.set(publisher, connector, workerPool, indexer, indexerThread);
}
```

When the connector finishes, the state is cleared. The `RunnerState.close()` method attempts orderly shutdown of each component, logging errors but not throwing.

## ConnectorResult and RunResult Reporting

`ConnectorResult` captures per-connector outcomes:
- Status (success/failure)
- Error message (if failed)
- Duration in seconds
- Document counts from the Publisher (succeeded, failed)

`RunResult` aggregates across all connectors:
- Overall status
- List of all ConnectorResults
- Summary message (e.g., "2/3 connectors complete. Some docs failed.")
- For TEST mode: a `Map<String, TestMessenger>` containing message history per connector

## The Connector Timeout Mechanism

Each connector has a configurable timeout (default: 24 hours):

```java
final int connectorTimeout = config.hasPath("runner.connectorTimeout") ?
    config.getInt("runner.connectorTimeout") : DEFAULT_CONNECTOR_TIMEOUT;
pubResult = publisher.waitForCompletion(connectorThread, connectorTimeout);
```

Inside `waitForCompletion`, the timeout is checked on each poll iteration:

```java
if (timeout > 0 && ChronoUnit.MILLIS.between(start, Instant.now()) > timeout) {
    return new PublisherResult(false, "Connector timeout.");
}
```

This prevents a stuck connector from blocking the entire run indefinitely.

## Sequential Connector Composition

Connectors execute strictly sequentially. The next connector only starts after the previous one fully completes (all documents indexed or failed):

```java
for (Connector connector : connectors) {
    ConnectorResult result = runConnectorWithComponents(...);
    if (!result.getStatus()) {
        log.error("Aborting run because " + connector.getName() + " failed.");
        return new RunResult(false, ...);
    }
}
```

Each connector gets its own WorkerPool, Indexer, and Publisher. Components from one connector are fully stopped before the next connector's components are created.

## The `main()` Method and CLI Options

```java
Options cliOptions = new Options()
    .addOption(Option.builder("usekafka").hasArg(false)
        .desc("Use Kafka for inter-component communication").build())
    .addOption(Option.builder("local").hasArg(false)
        .desc("Modifies useKafka mode to execute pipelines locally").build())
    .addOption(Option.builder("validate").hasArg(false)
        .desc("Validate the configuration and exit").build())
    .addOption(Option.builder("render").hasArg(false)
        .desc("Print out the configuration file with substitutions applied and exit").build());
```

| Flag | Effect |
|------|--------|
| (none) | `RunType.LOCAL` — full local execution with in-memory queues |
| `-usekafka` | `RunType.KAFKA_DISTRIBUTED` — only run connectors, assume external workers/indexers |
| `-usekafka -local` | `RunType.KAFKA_LOCAL` — local workers/indexers but communicate via Kafka |
| `-validate` | Validate config and exit (no execution) |
| `-render` | Print resolved config as JSON and exit |

The `-validate` and `-render` flags can be combined. All args are lowercased before parsing.

## Thread Model (Local Mode)

For each connector in a local run, there are 4+ threads:

1. **Main thread** — runs `waitForCompletion()`, polling for events
2. **ConnectorThread** — calls `connector.execute(publisher)`, publishes documents
3. **Worker thread(s)** — poll documents, run pipeline, emit results (configurable count)
4. **Indexer thread** — polls processed documents, sends to search engine

Plus a **WorkerPool watcher thread** that monitors worker health and logs statistics.
