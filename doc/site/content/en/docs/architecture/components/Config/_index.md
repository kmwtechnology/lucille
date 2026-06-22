---
title: Config
weight: 11
date: 2025-06-09
description: Why Lucille is configuration-driven, and how the choice of HOCON and Typesafe Config shapes the system.
---

## Configuration as an Architectural Principle

Lucille is a configuration-driven system. Every aspect of an ingest — which sources to read, which enrichment stages to apply, which search backend to write to, how many worker threads to run, what retry policy to use — is declared in a configuration file rather than hardcoded in application logic.

This is not merely a convenience. It is a fundamental architectural decision with consequences that ripple through the entire system:

**Separation of concerns.** The framework provides the execution engine; the configuration provides the instructions. A developer can change what an ingest does — add a stage, switch a connector, adjust batch sizes — without modifying or rebuilding any code. The same compiled JAR serves every ingest; only the config file changes.

**Composability.** Because ingests are defined declaratively, they can be composed from reusable pieces. A connector definition, a pipeline fragment, or a set of connection parameters can be defined once and included in multiple configs. This prevents drift and duplication across an organization's ingests.

**Validatability.** Because every component declares what configuration it expects (via the SPEC system), the entire config can be validated before any work begins. Errors are caught at startup — all at once, not one at a time — rather than surfacing mid-run after hours of processing.

**Environment portability.** The same config file works across development, staging, and production by substituting environment-specific values (URLs, credentials, index names) from environment variables at load time. No code changes, no separate config files per environment, no rebuild.

---

## Why HOCON and Typesafe Config

The choice of configuration format and library is not a peripheral implementation detail. In a system where every component — every Stage, Connector, and Indexer — receives its instructions through configuration, the config library is effectively the API between the user and the framework. Its capabilities and limitations shape what users can express and how the system behaves.

Lucille uses [HOCON](https://github.com/lightbend/config/blob/main/HOCON.md) (Human-Optimized Config Object Notation) parsed by the [Typesafe Config](https://github.com/lightbend/config) library. This choice provides several properties that matter architecturally:

**Comments.** A config file that defines an entire ingestion pipeline — potentially dozens of stages, multiple connectors, connection details, tuning parameters — requires annotation. JSON does not support comments. HOCON does. This is not a minor ergonomic preference; it is the difference between a config that is self-documenting and one that requires external documentation to understand.

**Environment variable substitution.** The `${?ENV_VAR}` syntax allows credentials and environment-specific values to be injected at load time without any application code involvement. This means the framework never needs to implement its own env-var resolution logic, and the precedence rules (file value vs. env var) are defined in one place — the config file itself — rather than scattered across component implementations.

**File includes.** HOCON's `include` directive enables config composition. Shared settings (connection strings, common pipeline fragments) are defined once and included everywhere. This is what makes it practical for an organization to maintain dozens of ingests against shared infrastructure without duplicating connection details that drift out of sync.

**Internal variable substitution.** HOCON supports references within a config file, so a value defined once can be reused across multiple blocks. Combined with environment variable substitution, this eliminates repetition and ensures consistency — change a value in one place and it propagates everywhere it's referenced.

**Relaxed syntax.** HOCON allows omitting quotes around keys, using `=` or `:` for assignment, and trailing commas. This makes configs more readable and less error-prone to edit by hand than strict JSON.

**List concatenation.** HOCON supports appending to lists and concatenating adjacent arrays. This enables patterns like composing a stages list from multiple included fragments — each fragment contributes its stages, and HOCON merges them into a single list at resolution time.

**Typed access with path expressions.** The Typesafe Config library provides `getString()`, `getInt()`, `getConfigList()`, and other typed accessors with dot-path navigation. This means component code reads configuration through a clean, typed API rather than parsing raw text. Type errors are caught at access time with clear messages.

**Automatic resolution order.** The library merges configuration from system properties, the application config file, and reference defaults in a defined precedence. This means any config value can be overridden via a system property (`-Dproperty=value`) without modifying the file — useful for one-off test runs and CI overrides.

**Config file selection at runtime.** The `-Dconfig.file` system property tells Lucille which config file to use. This keeps the application generic — the same JAR, the same classpath, the same entrypoint — with only the config file varying between runs. In containerized deployments, this means a single Docker image can serve any ingest by varying an environment variable at launch time.

These properties combine to make configuration a first-class architectural concern rather than an afterthought. The config file is not just "where you put settings" — it is the primary interface through which users interact with the framework, and its expressiveness directly determines how maintainable, portable, and correct an ingest can be.

---

## Practical Guides

For how to write a config file, see [Writing a Config]({{< relref "docs/reference/writing-a-config" >}}) in the Ingest Designer Guide.

For operational patterns — environment variable substitution, containerized deployment, config composition, and distributed mode configuration — see [Configuration Management]({{< relref "docs/operations/configuration" >}}) in the Operations Guide.
