---
title: Config
date: 2025-06-09
description: The Config is a HOCON file where you define the settings for running Lucille.
---

## Lucille Configuration

When you run Lucille, you provide a path to a file which provides configuration for your run. Configuration (Config) files use HOCON, 
a superset of JSON. This file defines all the components in your Lucille run. 

**Quick references**

* Example (local single-process): [application-example.conf](https://github.com/kmwtechnology/lucille/blob/main/application-example.conf)
* Example (S3 OpenSearch): [s3-opensearch.conf](https://github.com/kmwtechnology/lucille/blob/main/lucille-examples/lucille-s3-ingest-example/conf/s3-opensearch.conf)
* HOCON / Typesafe Config docs: [lightbend/config](https://github.com/lightbend/config)

A complete config file must contain three elements (Connector(s), Pipeline(s), Indexer):

### Connectors

Connectors read data from a source and emit it as a sequence of individual Documents, which will then be sent to a Pipeline for enrichment.

`connectors` should be populated with a list of Connector configurations. 

See [Connectors]({{< relref "docs/architecture/components/connectors" >}}) for more information about configuring Connectors.

### Pipeline and Stages

A pipeline is a list of Stages that will be applied to incoming Documents, preparing them for indexing.
As each Connector executes, the Documents it publishes can be processed by a Pipeline, made up of Stages. 

`pipelines` should be populated with a list of Pipeline configurations. Each Pipeline needs two values: `name`, 
the name of the Pipeline, and `stages`, a list of the Stages to use. Multiple connectors may feed to the same Pipeline. 

See [Stages]({{< relref "docs/architecture/components/stages" >}}) for more information about configuring Stages.

### Indexer

An indexer sends processed Documents to a specific destination. Only one Indexer can be defined; all pipelines will feed to the same Indexer.

A full indexer configuration has two separate config blocks: first, the generic `indexer` configuration, and second, configuration for the specific indexer
used in your run. For example, to use the `SolrIndexer`, you provide separate `indexer` and `solr` config blocks.

See [Indexers]({{< relref "docs/architecture/components/indexers" >}}) for more information about configuring your Indexer.

### Other Run Configuration

In addition to the three required elements, you can configure other parts of a Lucille run. The canonical reference for all available options — with comments explaining every field — is [application-example.conf](https://github.com/kmwtechnology/lucille/blob/main/application-example.conf).

| Block | Key Settings | Notes |
|---|---|---|
| `worker` | `threads`, `maxRetries`, `exitOnTimeout`, `maxProcessingSecs`, `enableHeartbeat` | Per-thread pipeline isolation. See [Worker]({{< relref "docs/architecture/components/worker" >}}). |
| `publisher` | `queueCapacity`, `maxPendingDocs` | Backpressure control. `queueCapacity` for local mode; `maxPendingDocs` for distributed. |
| `runner` | `metricsLoggingLevel`, `connectorTimeout` | `connectorTimeout` defaults to 24 hours. |
| `kafka` | `bootstrapServers`, `consumerGroupId`, `maxPollIntervalSecs`, `maxRequestSize`, `events`, `sourceTopic`, `eventTopic`, `documentDeserializer`, `documentSerializer`, `securityProtocol`, `consumerPropertyFile`, `producerPropertyFile`, `adminPropertyFile` | Required when running in distributed or Kafka-local mode. See [Deployment]({{< relref "docs/operations/deployment" >}}). |
| `zookeeper` | `connectString` | Required only when `worker.maxRetries` is set. |
| `log` | `seconds` | Controls how often Workers, Publisher, and Indexer log status updates and heartbeats. Default: 30. |

## Validation

Lucille validates the configuration for every Connector, Stage, and Indexer before starting a run. If you provide an unrecognized property, or omit a required one, Lucille throws an exception at startup rather than discovering the problem mid-run.

To validate your config without starting a run, use the `-validate` flag:

```bash
java -Dconfig.file=my-config.conf -cp '...' com.kmwllc.lucille.core.Runner -validate
```

All errors are reported together — not just the first one — so you can fix them all at once.

Each built-in component (Stage, Connector, Indexer, File Handler) declares a `Spec` that enumerates its legal configuration properties and their types. The validator checks your config against these specs. If you are building a new component, see [Developing New Components]({{< relref "docs/developer-guide/dev_new_components" >}}) for how to declare a `Spec`.