---
title: Components
weight: 4
description: >
  Conceptual guide to the core components of Lucille and how they work together.
---

Lucille is built from a small set of components that work together to move data from source systems into a search backend.

| Component | Role |
|---|---|
| [Connectors]({{< relref "Connectors" >}}) | Read data from a source system and emit Documents into the pipeline. |
| [Stages]({{< relref "Stages" >}}) | Process and enrich Documents. Composed into Pipelines. |
| [Indexers]({{< relref "Indexers" >}}) | Batch processed Documents and send them to the search backend. |
| [Pipeline]({{< relref "pipeline" >}}) | An ordered sequence of Stages applied to each Document. |
| [Worker]({{< relref "worker" >}}) | Pulls Documents from the source queue, runs them through the Pipeline, and forwards results. |
| [Publisher]({{< relref "publisher" >}}) | Tracks every Document from publication to terminal state; provides backpressure. |
| [Runner]({{< relref "run" >}}) | Orchestrates a complete run: validates config, starts components, waits for completion. |
| [Document]({{< relref "document/_index.md" >}}) | The basic unit of data flowing through the system. |
| [Config]({{< relref "Config" >}}) | The HOCON configuration file that defines all components and settings for a run. |

For a catalogue of specific implementations — built-in connectors, stages, indexers, file handlers, and plugins — see the [Reference]({{< relref "docs/reference" >}}) section.
