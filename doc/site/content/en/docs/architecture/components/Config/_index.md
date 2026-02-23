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

In addition to those three elements, you can also configure other parts of a Lucille run.
* `publisher` - Define the `queueCapacity`.
* `log`
* `runner`
* `zookeeper`
* `kafka` - Provide a `consumerPropertyFile`, `producerPropertyFile`, `adminPropertyFile`, and other configuration.
* `worker` - Control how many `threads` you want, the `maxRetries` in Zookeeper, and more.

## Validation

Lucille validates the Config you provide for Connectors, Stages, and Indexers. For example, in a Stage, if you provide a property 
the Stage does not use, an Exception will be thrown. An Exception will also be thrown if you do not provide a property required by
the Stage. 

If you want to validate your config file without starting an actual run, you can use our command-line validation tool. Just add 
`-validate` to the end of your command executing Lucille. The errors with your config will be printed out to the console, and
no actual run will take place.

### Config Validation
Lucille components (like Stages, Indexers, and Connectors) each take in a set of specific arguments to configure the component correctly.
Sometimes, certain properties are required - like the `paths` for your `FileConnector` traversal, or the `path` for your
`CSVIndexer`. Other properties are optional / do not always have to be specified.

For these components, developers must declare a `Spec` which defines the properties that are required or optional. They must
also declare what type each property is (number, boolean, string, etc.). For example, the `SequenceConnector` requires you
to specify the `numDocs` you want to create, and optionally, the number you want IDs to `startWith`. So, the `Spec` looks like this:

```java
public static final Spec SPEC = SpecBuilder.connector()
      .requiredNumber("numDocs")
      .optionalNumber("startWith")
      .build();
```

### Declaring a Spec

Lucille is designed to access Specs reflectively. If you build a Stage/Indexer/Connector/File Handler, you need to declare a `public static Spec` 
named `SPEC` (exactly). Failure to do so will not result in a _compile-time_ error. However, you will not be able
to instantiate your component - even in unit tests - as the reflective access (which takes place in the super / abstract class) will always fail.

When you declare the `public static Spec SPEC`, you'll want to call the appropriate `SpecBuilder` method which provides appropriate
default arguments for your component. For example, if you are building a Stage, you should call `SpecBuilder.stage()`, which allows
the config to include `name`, `class`, `conditions`, and `conditionPolicy`. 

### Lists and Objects

Validating a list / object is a bit tricky. When you declare a required / optional list or object in a Config, you can either
provide:

1. A `TypeReference` describing what the unwrapped List/Object should deserialize/cast to.
2. A `Spec`, for a list, or a named Spec (created via SpecBuilder.parent()), for an object, describing the valid properties. (Use a `Spec` for a list when you need a `List<Config>` with specific structure. For example, `Stage` conditions.)

### Parent / Child Validation

Some configs include properties which are objects, containing _even more_ properties. For example, in the `FileConnector`, you
can specify `fileOptions` - which includes a variety of additional arguments, like `getFileContent`, `handleArchivedFiles`, and more. 
This is defined in a parent Spec, created via SpecBuilder.parent(), which has a name (the key the config is held under) and has its own required/optional properties.
The `fileOptions` parent `Spec` is:

```java
SpecBuilder.parent("fileOptions")
  .optionalBoolean("getFileContent", "handleArchivedFiles", "handleCompressedFiles")
  .optionalString("moveToAfterProcessing", "moveToErrorFolder").build();
```

A parent `Spec` can be either required or optional. When the parent is present, its properties will be validated against this parent `Spec`.

There will be times that you can't know what the field names would be in advance. For example, a field mapping of some kind.
In this case, you should pass in a `TypeReference` describing what type the unwrapped `ConfigObject` should deserialize/cast to.
For example, if you want a field mapping of Strings to Strings, you'd pass in `new TypeReference<Map<String, String>>(){}`.
In general, you should use a `Spec` when you know field names, and a `TypeReference` when you don't.

### Why Validate?

Obviously, you will get an error if you call `config.getString("field")` when `"field"` is not specified. However, additional validation
on Configs is still useful/necessary for two primary reasons:

1. **Command-line utility**

We want to allow the command-line validation utility to provide a comprehensive list of a Config's errors. As such, Lucille has to
validate the config before a Stage/Connector/Indexer begins accessing properties and potentially throwing a `ConfigException`.

2. **Prevent typos from ruining your pipeline**

A mistyped field name could have massive ripple effects throughout your pipeline. As such, each Stage/Connector/Indexer needs to
have a specific set of legal Config properties, so Exceptions can be raised for unknown or unrecognized properties.