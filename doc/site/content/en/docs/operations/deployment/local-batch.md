---
title: "Local Batch"
weight: 1
date: 2025-06-09
description: Running all Lucille components in a single JVM with in-memory queues.
---

## Local Mode

In local mode, you start a single Java process at the command line, invoking the `main()` method of `com.kmwllc.lucille.core.Runner`. Alternatively, if you are using Lucille as a dependency in your own Java project, you call `Runner.run()` from your code.

All components (Connector, Workers, Indexer) run as threads in a single JVM. Communication uses in-memory queues. There is no external infrastructure required — no Kafka, no ZooKeeper, no separate processes.

**Best for:** Development, scheduled batch jobs, single-machine production runs.

> **Note:** While local mode is the first mode you'd want to use when developing a pipeline or doing a proof-of-concept, it is not at all a "toy" mode. It is fully suitable for production batch workflows assuming you don't need the additional parallelism and crash resilience features of distributed mode.

### Prerequisites

To run Lucille in local mode you need:

1. **A Lucille configuration file** — specifies connectors, pipelines, stages, and indexer settings. See [Configuration]({{< relref "docs/operations/configuration" >}}) for details.
2. **Lucille and its dependencies on the classpath** — after a source build, `lucille-all/target/lib/` holds lucille-core, lucille-logging, every plugin and their dependencies. Or use the `kmwtechnology/lucille-all` Docker image, which sets up the classpath for you.

### Running from the Command Line

A typical invocation looks like:

```bash
java \
  -Xmx4g \
  -Dlog4j2.configurationFile=classpath:lucille-log4j2.xml \
  -Dconfig.file=/path/to/application.conf \
  -cp 'lucille-all/target/lib/*' \
  com.kmwllc.lucille.core.Runner
```

### Running Programmatically

If Lucille is a dependency in your own project, you can trigger a run from Java code:

```java
Config config = ConfigFactory.load("application.conf");
RunResult result = Runner.run(config, Runner.RunType.LOCAL);
if (!result.getStatus()) {
    System.err.println(result.getMessage());
}
```

To also log the run summary and metrics as the command-line Runner does, call `Runner.runAndLogResult(config, Runner.RunType.LOCAL, true)` instead.

### Scaling in Local Mode

In local mode, the main way to scale is to increase the number of Worker threads. This allows you to:

- Do more I/O concurrently (if the pipeline is I/O-bound)
- Take advantage of available CPU cores for pipeline processing

Each Worker thread runs an independent copy of the pipeline, and the Worker threads together process documents concurrently.

```hocon
worker {
  threads: 8
}
```
