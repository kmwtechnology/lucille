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
| [Document Model]({{< relref "document-model" >}}) | Why the Document is backed by a Jackson ObjectNode, the API design choices, and the tradeoffs |
| [Pipeline Internals]({{< relref "pipeline-internals" >}}) | How lazy iterator chaining works, why in-place modification, memory implications |
| [Messenger Abstraction]({{< relref "messenger-abstraction" >}}) | The interfaces that make deployment-mode independence possible |
| [Publisher Accounting]({{< relref "publisher-accounting" >}}) | The Bag data structure, out-of-order events, completion detection |
| [Runner Orchestration]({{< relref "runner-orchestration" >}}) | How Runner.run() coordinates the full lifecycle |
| [Message Ordering]({{< relref "message-ordering" >}}) | How Kafka keys preserve operation order across distributed components |
| [Error Handling]({{< relref "error-handling" >}}) | The error philosophy, every failure scenario, and fault tolerance |
| [Kafka Integration]({{< relref "kafka-integration" >}}) | Topics, serialization, consumer groups, offset strategies |
| [Metrics and Observability]({{< relref "metrics-observability" >}}) | Codahale metrics, the watcher thread, heartbeats, MDC |
