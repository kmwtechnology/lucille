---
title: Internals
weight: 10
aliases:
  - /docs/internals/
description: >
  In-depth explanations of how each architectural subsystem works internally and why it was designed that way.
---

These pages go beyond the component reference to explain the internal mechanics of each subsystem. They are useful for developers who need to understand *why* the system behaves the way it does, debug unexpected behavior, or evaluate whether Lucille's design fits their use case.

Each page is self-contained — you can read them in any order based on what you need to understand.

| Page | What It Explains |
|---|---|
| [Messenger Abstraction]({{< relref "messenger-abstraction" >}}) | The interfaces that make deployment-mode independence possible |
| [Message Ordering]({{< relref "message-ordering" >}}) | How Kafka keys preserve operation order across distributed components |
| [Error Handling]({{< relref "error-handling" >}}) | The error philosophy, every failure scenario, and fault tolerance |
| [Kafka Integration]({{< relref "kafka-integration" >}}) | Topics, serialization, consumer groups, offset strategies |
| [Metrics and Observability]({{< relref "metrics-observability" >}}) | Codahale metrics, the watcher thread, heartbeats, MDC |

Deep dives that have been merged into their component pages:
- [Document Model]({{< relref "docs/architecture/components/document/document-model" >}}) — now under Components > Document
- [Pipeline Internals]({{< relref "docs/architecture/components/pipeline/pipeline-internals" >}}) — now under Components > Pipeline
- [Publisher Accounting]({{< relref "docs/architecture/components/publisher/publisher-accounting" >}}) — now under Components > Publisher
- [Runner Orchestration]({{< relref "docs/architecture/components/runner/runner-orchestration.md" >}}) — now under Components > Runner
