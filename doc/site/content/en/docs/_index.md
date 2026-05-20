---
title: Documentation
linkTitle: Docs
menu: {main: {weight: 20}}
weight: 20
---

Lucille is a Java-based search ETL framework that reads data from sources, enriches it through configurable pipelines, and delivers it to search engines and vector databases.

## Find Your Section

| Section | Who It's For | What You'll Find |
|---|---|---|
| [Getting Started]({{< relref "getting-started" >}}) | Anyone new to Lucille | Installation, first pipeline tutorial, and why Lucille exists. |
| [Architecture]({{< relref "architecture" >}}) | Anyone who wants to understand *why* the system is designed the way it is | Design motivations, guiding principles, component roles and interactions, and deep dives into internal mechanisms. The aim is conceptual understanding. |
| [Ingest Designer Guide]({{< relref "reference" >}}) | Someone writing Lucille configs to define ingests — no Java required | How to write a config, pipeline definition patterns, the full catalogue of connectors/stages/indexers, conditions reference, and task-oriented cookbooks. |
| [Component Developer Guide]({{< relref "developer-guide" >}}) | Java developers implementing new Connectors, Stages, or Indexers | What the framework provides vs. what you implement, skeleton code, the SPEC validation system, testing patterns, and API quick reference. |
| [Operations Guide]({{< relref "operations" >}}) | Someone deploying and running Lucille in dev, staging, or production | Deployment modes (local, distributed, streaming, hybrid), configuration management, performance tuning, logging, security, troubleshooting, and the support matrix. |
| [Contributor Guide]({{< relref "contributing" >}}) | Someone making a PR against the Lucille codebase | Project structure, coding conventions, setup standards, and how the project is maintained. |

Also available: [FAQ]({{< relref "faq" >}}), [Glossary]({{< relref "glossary" >}}), [Releases]({{< relref "releases" >}}).

---

## For Documentation Contributors

Each section targets a specific reader with a specific goal. When adding or moving content, use this principle:

- **Architecture** answers "why" and "how it works" — design decisions, rationale, component interactions. No config syntax tutorials, no step-by-step instructions.
- **Ingest Designer Guide** answers "how do I configure this" — practical reference for someone assembling an ingest from existing components. Config examples, parameter tables, gotchas, cookbooks.
- **Component Developer Guide** answers "how do I build this" — guidance for someone writing Java code against Lucille's interfaces. Code skeletons, API patterns, testing infrastructure.
- **Operations Guide** answers "how do I deploy and run this" — deployment modes, environment setup, monitoring, troubleshooting. Operational patterns and production concerns.
- **Contributor Guide** answers "how do I contribute to the project" — coding standards, project structure, PR process.

If content serves multiple audiences, place it where the *primary* audience would look and add a cross-link from the secondary location.
