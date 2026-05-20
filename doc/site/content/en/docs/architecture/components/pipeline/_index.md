---
weight: 7
title: Pipeline
date: 2024-10-15
description: The ordered sequence of Stages that transform Documents before they are indexed.
---

A *Pipeline* is an ordered sequence of processing Stages. When a Connector publishes a Document, that Document is picked up by a Worker and passed through every Stage in the configured Pipeline before being sent to the Indexer.

## Linear Execution Model

Stages execute in the order they are listed. Each Stage receives the Document as mutated by all previous Stages. If a Stage generates child documents, those children flow through the remaining stages of the same pipeline independently.

This is a deliberate architectural choice: a pipeline is a linear sequence, not an arbitrary graph. There are no branches, no sub-pipelines, and no conditional routing to different pipeline paths.

## Why No Sub-Pipelines

Experience has shown that sub-pipelines and branching graphs introduce significant cognitive and testing complexity. They make pipelines harder to reason about and harder to troubleshoot — before you can diagnose a problem, you first have to determine which route a document took. In most real-world ingestion scenarios, sub-pipelines do not turn out to be necessary.

Lucille provides three mechanisms that cover the cases where branching might seem attractive:

**Conditions.** Every stage supports a `conditions` block in configuration that determines whether that stage should process a given document. Conditions can check field presence, field values, or combinations (with `all`/`any` policy). The framework evaluates conditions before invoking the stage — stage authors never implement conditional logic themselves. This allows a single linear pipeline to apply different processing to different document types without branching.

**Custom stages for complex logic.** If you need decision logic more complex than what conditions can express, the pathway is to write a custom stage where that logic is implemented and tested in Java — or in Python using Lucille's `EmbeddedPython` or `ExternalPython` stages. A custom stage can inspect any aspect of the document and take arbitrary action, including setting fields that downstream conditions can check.

**Config includes for reuse.** If the concern is reusing common sequences of stages across multiple pipelines, those sequences can be defined in a separate config file and composed into a larger pipeline definition using HOCON's array concatenation. The pipeline remains a single linear sequence at execution time — the composition happens at config resolution time, not at runtime.

## Per-Thread Isolation

When multiple Worker threads are active, **each thread gets its own Pipeline instance** — and its own instance of every Stage. This means:

- Stages can safely hold stateful resources (database connections, loaded models, compiled patterns) without synchronization.
- Expensive setup (model loading, connection pool creation) happens once per Worker thread at startup.
- Per-thread isolation eliminates a whole class of concurrency bugs in Stage implementations.

This design means that pipeline authors write sequential code — read a field, transform it, write a field — and the framework handles parallelism. The complexity of concurrent execution is the framework's responsibility, not the user's.

## Multiple Pipelines

Multiple pipelines can be defined in a single run, each serving different connectors. All pipelines feed the same Indexer. This allows a single Lucille invocation to ingest from multiple sources with different enrichment logic, all writing to the same search backend.

---

## Practical Guide

For how to define pipelines in configuration — syntax, connecting connectors, conditions, reuse patterns, and examples — see [Defining Pipelines]({{< relref "docs/reference/defining-pipelines" >}}) in the Ingest Designer Guide.
