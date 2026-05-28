---
title: Developing New Components
weight: 10
date: 2024-10-28
description: >
  The basics of how to develop Connectors, Stages, and Indexers for Lucille.
---

Each component type has its own dedicated guide:

- [Developing Stages]({{< relref "docs/developer-guide/developing-stages" >}}) — Skeleton, lifecycle, conditional execution, and the Document API.
- [Developing Connectors]({{< relref "docs/developer-guide/developing-connectors" >}}) — Skeleton, lifecycle, and publishing documents.
- [Developing Indexers]({{< relref "docs/developer-guide/developing-indexers" >}}) — Skeleton, lifecycle, and sending documents to a destination.

All components must declare a [SPEC]({{< relref "docs/developer-guide/spec-validation" >}}) and follow the [Javadoc Standards]({{< relref "docs/developer-guide/javadocs" >}}). See [Testing Pipelines]({{< relref "docs/developer-guide/testing" >}}) for testing conventions.
