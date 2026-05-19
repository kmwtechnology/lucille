---
weight: 7
title: Pipeline
date: 2024-10-15
description: The ordered sequence of Stages that transform Documents before they are indexed.
---

A *Pipeline* is an ordered sequence of processing Stages. When a Connector publishes a Document, that Document is picked up by a Worker and passed through every Stage in the configured Pipeline before being sent to the Indexer.

## Defining a Pipeline

Pipelines are defined in the `pipelines` list in your config file. Each pipeline requires a `name` and a `stages` list:

```hocon
pipelines: [
  {
    name: "my-pipeline"
    stages: [
      {
        class: "com.kmwllc.lucille.stage.RenameFields"
        fieldMapping: { old_name: new_name }
      },
      {
        class: "com.kmwllc.lucille.stage.TrimWhitespace"
        fields: ["title", "description"]
      }
    ]
  }
]
```

## Connecting a Connector to a Pipeline

Each Connector specifies which pipeline will process its output via the `pipeline` field:

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

Multiple Connectors can feed the same Pipeline. Multiple Pipelines can be defined in a single run.

## Stage Execution Order

Stages execute in the order they are listed. Each Stage receives the Document as mutated by all previous Stages. If a Stage generates child documents, those children flow through the remaining stages of the same pipeline independently.

## Conditional Stage Execution

Every Stage supports a `conditions` block that controls when the Stage applies to a given Document. See [Stages]({{< relref "docs/architecture/components/stages" >}}) for full documentation on conditions.

```hocon
{
  class: "com.kmwllc.lucille.stage.OpenAIEmbed"
  fields: ["content"]
  conditions: [
    { fields: ["content_type"], values: ["article"] }
  ]
}
```

In this example, `OpenAIEmbed` only runs on Documents where `content_type` equals `"article"`.

## No Sub-Pipelines

A pipeline is a linear sequence of stages — not an arbitrary graph. Documents cannot branch in different directions, and a pipeline cannot invoke another pipeline mid-execution.

This is a deliberate design choice. Experience has shown that sub-pipelines and branching graphs introduce significant cognitive and testing complexity. They make pipelines harder to reason about and harder to troubleshoot — before you can diagnose a problem, you first have to determine which route a document took. In most real-world ingestion scenarios, sub-pipelines do not turn out to be necessary.

Lucille provides three mechanisms that cover the cases where branching might seem attractive:

**Conditions.** Every stage supports a `conditions` block in configuration that determines whether that stage should process a given document. Conditions can check field presence, field values, or combinations (with `all`/`any` policy). The framework evaluates conditions before invoking the stage — stage authors never implement conditional logic themselves. This allows a single linear pipeline to apply different processing to different document types without branching.

**Custom stages for complex logic.** If you need decision logic more complex than what conditions can express, the pathway is to write a custom stage where that logic is implemented and tested in Java — or in Python using Lucille's `EmbeddedPython` or `ExternalPython` stages. A custom stage can inspect any aspect of the document and take arbitrary action, including setting fields that downstream conditions can check.

**Config includes for reuse.** If the concern is reusing common sequences of stages across multiple pipelines, those sequences can be defined in a separate config file and composed into a larger pipeline definition using HOCON's array concatenation:

```hocon
include "shared-stages.conf"   # defines: shared_stages = [{...}, {...}]

pipelines: [{
  name: "my-pipeline"
  stages = [{ name: "first_stage", class: "..." }] ${shared_stages} [{ name: "last_stage", class: "..." }]
}]
```

HOCON concatenates adjacent arrays into a single list, so the resolved `stages` array contains `first_stage`, followed by the shared stages, followed by `last_stage`. This provides composition and reuse without runtime branching — the pipeline remains a single linear sequence at execution time.

## Per-Thread Isolation

When multiple Worker threads are active, **each thread gets its own Pipeline instance** — and its own instance of every Stage. This means:

- Stages can safely hold stateful resources (database connections, loaded models, compiled patterns) initialized in `start()` without synchronization.
- Expensive setup (model loading, connection pool creation) happens once per Worker thread at startup.
- Per-thread isolation eliminates a whole class of concurrency bugs in Stage implementations.

## Multiple Pipelines in a Single Run

```hocon
connectors: [
  { name: "csv-connector",  class: "...", pipeline: "csv-pipeline" },
  { name: "json-connector", class: "...", pipeline: "json-pipeline" }
]

pipelines: [
  { name: "csv-pipeline",  stages: [...] },
  { name: "json-pipeline", stages: [...] }
]
```

All Pipelines feed the same Indexer.

## Empty Pipeline

A Pipeline with an empty `stages` list is valid. Documents pass through without transformation.

```hocon
pipelines: [
  { name: "passthrough", stages: [] }
]
```

## Building From Code

Pipelines can also be constructed programmatically for integration tests:

```java
Config config = ConfigFactory.load("my-config.conf");
Pipeline pipeline = Pipeline.fromConfig(config, "my-pipeline");
pipeline.startStages();

Iterator<Document> results = pipeline.processDocument(doc);

pipeline.stopStages();
```
