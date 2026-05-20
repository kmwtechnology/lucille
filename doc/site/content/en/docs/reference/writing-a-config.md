---
title: "Writing a Config"
weight: 1
date: 2025-06-09
description: >
  How to write a Lucille configuration file — structure, required elements, and available settings.
---

When you run Lucille, you provide a path to a configuration file that defines your entire ingest: which sources to read from, which pipelines to apply, and where to send the results. Configuration files use HOCON, a superset of JSON.

**Quick references:**

* [s3-opensearch.conf](https://github.com/kmwtechnology/lucille/blob/main/lucille-examples/lucille-s3-ingest-example/conf/s3-opensearch.conf) — A simple, runnable example that ingests files from S3 into OpenSearch.
* [application-example.conf](https://github.com/kmwtechnology/lucille/blob/main/application-example.conf) — A comprehensive, annotated illustration of every top-level property and block that a Lucille config can contain (worker, publisher, kafka, indexer, etc.), excluding the implementation-specific parameters of individual stages, connectors, and indexers. Use this as a reference when you need to know what options exist and what they do. Note: this file is a reference document, not a tested runnable config.
* [lightbend/config](https://github.com/lightbend/config) — The upstream documentation for HOCON syntax and the Typesafe Config library that Lucille uses to parse configs. Consult this when you need the definitive rules on substitution syntax, include semantics, value concatenation, or other HOCON language features beyond what this guide covers.

---

## Required Elements

A complete config file must contain three elements:

### Connectors

Connectors read data from a source and emit it as a sequence of individual Documents, which are then sent to a Pipeline for enrichment.

`connectors` should be populated with a list of Connector configurations.

```hocon
connectors: [
  {
    name: "my-connector"
    class: "com.kmwllc.lucille.connector.FileConnector"
    pipeline: "my-pipeline"
    paths: ["/data/files"]
  }
]
```

See [Connectors]({{< relref "docs/reference/connectors" >}}) for the full catalogue of available connectors and their parameters.

### Pipelines and Stages

A pipeline is a list of Stages applied to incoming Documents, preparing them for indexing. Each Connector specifies which pipeline will process its output via the `pipeline` field.

`pipelines` should be populated with a list of Pipeline configurations. Each Pipeline needs a `name` and a `stages` list. Multiple connectors may feed the same Pipeline.

```hocon
pipelines: [
  {
    name: "my-pipeline"
    stages: [
      { class: "com.kmwllc.lucille.stage.RenameFields", fieldMapping: { old_name: new_name } }
      { class: "com.kmwllc.lucille.stage.TrimWhitespace", fields: ["title", "description"] }
    ]
  }
]
```

See [Stages]({{< relref "docs/reference/stages" >}}) for the full catalogue of available stages and their parameters.

### Indexer

An Indexer sends processed Documents to a destination — typically a search engine. Only one Indexer can be defined per run; all pipelines feed to the same Indexer.

A full indexer configuration has two separate config blocks: the generic `indexer` block and a backend-specific block (e.g., `solr`, `opensearch`, `elastic`).

```hocon
indexer {
  type: "OpenSearch"
  batchSize: 100
  batchTimeout: 500
}

opensearch {
  url: "https://localhost:9200"
  index: "my-index"
}
```

See [Indexers]({{< relref "docs/reference/indexers" >}}) for available indexer types and their configuration.

---

## Other Run Configuration

In addition to the three required elements, you can configure other parts of a Lucille run. See [application-example.conf](https://github.com/kmwtechnology/lucille/blob/main/application-example.conf) for the complete annotated reference of all available top-level options.

| Block | Key Settings | Notes |
|---|---|---|
| `worker` | `threads`, `maxRetries`, `exitOnTimeout`, `maxProcessingSecs`, `enableHeartbeat` | Per-thread pipeline isolation. |
| `publisher` | `queueCapacity`, `maxPendingDocs` | Backpressure control. `queueCapacity` for local mode; `maxPendingDocs` for distributed. |
| `runner` | `metricsLoggingLevel`, `connectorTimeout` | `connectorTimeout` defaults to 24 hours. |
| `kafka` | `bootstrapServers`, `consumerGroupId`, `maxPollIntervalSecs`, `maxRequestSize`, `events`, `sourceTopic`, `eventTopic`, security properties | Required when running in distributed or Kafka-local mode. See [Deployment]({{< relref "docs/operations/deployment" >}}). |
| `zookeeper` | `connectString` | Required only when `worker.maxRetries` is set. |
| `log` | `seconds` | Controls how often Workers, Publisher, and Indexer log status updates. Default: 30. |

---

## Validation

Lucille validates the configuration for every Connector, Stage, and Indexer before starting a run. If you provide an unrecognized property, or omit a required one, Lucille throws an exception at startup rather than discovering the problem mid-run.

To validate your config without starting a run, use the `-validate` flag:

```bash
java -Dconfig.file=my-config.conf -cp '...' com.kmwllc.lucille.core.Runner -validate
```

All errors are reported together — not just the first one — so you can fix them all at once.

To see the fully resolved config (with all substitutions applied), use the `-render` flag:

```bash
java -Dconfig.file=my-config.conf -cp '...' com.kmwllc.lucille.core.Runner -render
```

---

## HOCON Basics for Ingest Designers

HOCON is a superset of JSON with features that make config files more readable and maintainable:

- **Comments** — use `#` or `//` to annotate your config
- **Relaxed syntax** — quotes around keys are optional, trailing commas are allowed
- **Environment variable substitution** — inject credentials and environment-specific values without hardcoding them:

```hocon
opensearch {
  url: "http://localhost:9200"
  url: ${?OPENSEARCH_URL}   # overrides with env var if set
}
```

- **Includes** — compose configs from reusable fragments:

```hocon
include "shared-stages.conf"
```

- **Internal references** — reuse values defined elsewhere in the same file:

```hocon
common { batchSize: 1000 }
indexer { batchSize: ${common.batchSize} }
```

For a deeper treatment of HOCON patterns, environment variable substitution, and config composition, see [Configuration Management]({{< relref "docs/operations/configuration" >}}) in the Operations Guide.
