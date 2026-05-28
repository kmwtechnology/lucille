---
title: "Defining Pipelines"
weight: 2
date: 2025-06-09
description: >
  How to define pipelines in a Lucille config — syntax, connecting connectors, multiple pipelines, conditions, and reuse patterns.
---

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

Stages execute in the order they are listed. Each Stage receives the Document as mutated by all previous Stages.

---

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

Multiple Connectors can feed the same Pipeline.

---

## Multiple Pipelines in a Single Run

You can define multiple pipelines in a single config, each serving different connectors:

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

---

## Empty Pipeline

A Pipeline with an empty `stages` list is valid. Documents pass through without transformation and are sent directly to the Indexer:

```hocon
pipelines: [
  { name: "passthrough", stages: [] }
]
```

---

## Conditional Stage Execution

Every Stage supports a `conditions` block that controls whether the Stage applies to a given Document. Conditions can check field presence, field values, or combinations:

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

Use `conditionPolicy` to control how multiple conditions combine:
- `"all"` (default) — all conditions must be true
- `"any"` — at least one condition must be true

See [Stages]({{< relref "docs/reference/stages" >}}) for the full conditions reference.

---

## Reusing Stage Sequences Across Pipelines

If you want to share a common sequence of stages across multiple pipelines, define them in a separate config file and compose them using HOCON's array concatenation:

```hocon
include "shared-stages.conf"   # defines: shared_stages = [{...}, {...}]

pipelines: [{
  name: "my-pipeline"
  stages = [{ name: "first_stage", class: "..." }] ${shared_stages} [{ name: "last_stage", class: "..." }]
}]
```

HOCON concatenates adjacent arrays into a single list, so the resolved `stages` array contains `first_stage`, followed by the shared stages, followed by `last_stage`.

---

## Testing Your Ingest

You can verify that your pipeline produces the expected output by running it in test mode. Lucille's test mode executes the full pipeline end-to-end — real connectors, real stages, real document routing — but bypasses the search backend and captures all output in memory for assertion. This requires writing a short Java test. See [Testing Pipelines]({{< relref "docs/developer-guide/testing" >}}) for a complete guide.

For config-only validation (no Java required), use the `-validate` flag described in [Writing a Config]({{< relref "docs/reference/writing-a-config" >}}).

---

## Building a Pipeline From Code

For integration tests or programmatic usage, pipelines can be constructed from a Config object:

```java
Config config = ConfigFactory.load("my-config.conf");
Pipeline pipeline = Pipeline.fromConfig(config, "my-pipeline");
pipeline.startStages();

Iterator<Document> results = pipeline.processDocument(doc);

pipeline.stopStages();
```

See [Testing Pipelines]({{< relref "docs/developer-guide/testing" >}}) for more on testing patterns.
