---
title: Config
date: 2025-06-09
description: How to configure and validate your configuration for a Lucille run.
---

### Lucille Configuration

When you run Lucille, you provide a path to a file which provides configuration for your run. Configuration (Config) files use HOCON, 
a superset of JSON. This file defines all the components in your Lucille run. 

A complete config file must contain three elements:

##### Connectors

Connectors read data from a source and emit it as a sequence of individual Documents, which will then be sent to a Pipeline for enrichment.

`connectors` should be populated with a list of Connector configurations. 

See [Connectors](Connectors/_index.md) for more information about configuring Connectors.

##### Pipelines

A pipeline is a list of Stages that will be applied to incoming Documents, preparing them for indexing.
As each Connector executes, the Documents it publishes can be processed by a Pipeline, made up of Stages. 

`pipelines` should be populated with a list of Pipeline configurations. Each Pipeline needs two values: `name`, 
the name of the Pipeline, and `stages`, a list of the Stages to use. Multiple connectors may feed to the same Pipeline. 

See [Stages](Stages/_index.md) for more information about configuring Stages.

##### Indexer

An indexer sends processed Documents to a specific destination. Only one Indexer can be defined; all pipelines will feed to the same Indexer.

A full indexer configuration has two parts: first, the generic `indexer` configuration, and second, configuration for the specific indexer
used in your run. For example, to use the `SolrIndexer`, you provide `indexer` and `solr` config.

See [Indexers](Indexers/_index.md) for more information about configuring your Indexer.

### Other Run Configuration

In addition to those three elements, you can also configure other parts of a Lucille run.
* `publisher` - Define the `queueCapacity`.
* `log`
* `runner`
* `zookeeper`
* `kafka` - Provide a `consumerPropertyFile`, `producerPropertyFile`, `adminPropertyFile`, and other configuration.
* `worker` - Control how many `threads` you want, the `maxRetries` in Zookeeper, and more.

### Validation

Lucille validates the Config you provide for Connectors, Stages, and Indexers. For example, in a Stage, if you provide a property 
the Stage does not use, an Exception will be thrown. An Exception will also be thrown if you do not provide a property required by
the Stage. 

If you want to validate your config file without starting an actual run, you can use our command-line validation tool. Just add 
`-validate` to the end of your command executing Lucille. The errors with your config will be printed out to the console, and
no actual run will take place.