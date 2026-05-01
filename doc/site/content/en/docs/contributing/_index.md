---
title: Contribution Guidelines
weight: 10
description: >
  How to develop new features and coding standards.
---

This section covers how to contribute to Lucille and how to develop new components.

## Guides

- **[Setup & Standards]({{< relref "docs/contributing/setup_standards" >}})** — Local developer setup and coding standards (IntelliJ code formatting, file exclusions).
- **[Developing New Components]({{< relref "docs/contributing/dev_new_components" >}})** — How to build new Stages, Connectors, and Indexers, including skeleton code, testing standards, and Javadoc requirements.

## Quick Start

1. Fork and clone the [Lucille repository](https://github.com/kmwtechnology/lucille).
2. Follow the [installation guide]({{< relref "docs/getting-started/installation" >}}) to build from source.
3. Set up IntelliJ with the Google code formatting scheme (see [Setup & Standards]({{< relref "docs/contributing/setup_standards" >}})).
4. Pick a component type and follow the skeleton in [Developing New Components]({{< relref "docs/contributing/dev_new_components" >}}).
5. Write tests following the testing standards.
6. Open a pull request.

## Contribution Locations

| Component | Location |
|---|---|
| Core Stages | `lucille-core/src/main/java/com/kmwllc/lucille/stage/` |
| Core Connectors | `lucille-core/src/main/java/com/kmwllc/lucille/connector/` |
| Core Indexers | `lucille-core/src/main/java/com/kmwllc/lucille/indexer/` |
| Plugin modules | `lucille-plugins/<plugin-name>/` |
| Tests | `src/test/java/` mirroring the main source layout |
| Test resources | `src/test/resources/<ComponentName>Test/` |
