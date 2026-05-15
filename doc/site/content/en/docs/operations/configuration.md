---
title: "Configuration Management"
weight: 1
date: 2025-06-09
description: >
  Why HOCON, how environment variable substitution works, config composition patterns, and pre-run validation.
---

## Why Configuration Is a Critical Design Choice in Search ETL

In a search ETL framework, configuration touches everything. A single Lucille config file defines:

- Which connectors to run and in what order
- Connection details for each source system
- Which pipeline each connector feeds
- The sequence of stages in each pipeline, with per-stage parameters
- Connection details for the search backend
- Indexer behavior (batch size, timeout, field filtering, retry policy)
- Worker thread count and timeout settings
- Kafka connection and topic configuration (in distributed mode)
- Publisher backpressure settings
- Logging and metrics intervals

A typical production config file interacts with 10–20 distinct components, each with their own parameters. Every Stage, Connector, and Indexer reads from the config. The config library is not a peripheral choice — it is the API through which every component receives its instructions.

If the config format is awkward, every component suffers. If the config library doesn't handle environment variable substitution cleanly, every deployment requires workarounds. If configs can't be composed from smaller files, organizations running multiple ingests against the same infrastructure end up with duplicated connection strings that drift out of sync.

---

## Why Environment Variable Substitution Matters

In containerized deployments (Docker, Kubernetes), credentials and environment-specific settings are typically provided via container environment variables or mounted secrets. A database password, an API key, a search engine URL — these should not be hardcoded in a config file that lives in version control.

The naive approach is to write explicit fallback logic in the application code:

```java
// DON'T DO THIS
String url = config.hasPath("opensearch.url") 
    ? config.getString("opensearch.url") 
    : System.getenv("OPENSEARCH_URL");
```

This is wrong for several reasons:
- Every component that reads a credential needs this fallback logic.
- The precedence rules (config vs. env var) are scattered across the codebase.
- It's impossible to tell from the config file alone what values will actually be used at runtime.
- Testing requires setting environment variables rather than just providing a test config.

The right approach is to delegate this entirely to the config library. The config file declares where environment variables should be substituted, and the library handles the resolution at load time. The application code simply calls `config.getString("opensearch.url")` and gets the resolved value — it never needs to know whether that value came from the file or an environment variable.

Lucille achieves this through HOCON's substitution syntax:

```hocon
opensearch {
  # default value in the file; overridden by env var if present
  url: "http://localhost:9200"
  url: ${?OPENSEARCH_URL}
}
```

The `${?OPENSEARCH_URL}` syntax means: "if the environment variable `OPENSEARCH_URL` is set, use its value; otherwise, keep the previous value." The second line overrides the first only when the env var is present. This is a single mechanism that handles:

- Development (use the default `localhost` value)
- CI/CD (set the env var in the test environment)
- Production containers (inject via Kubernetes secrets or Docker env vars)

No application code is involved. The config library resolves everything before any Lucille component sees it.

---

## Why HOCON Over JSON

Lucille uses HOCON (Human-Optimized Config Object Notation) via the Typesafe Config library. HOCON is a superset of JSON — any valid JSON is valid HOCON — but it adds features that prove essential in practice:

### Comments

JSON does not allow comments. In a config file that defines an entire ingestion pipeline, comments are not optional — they explain why a batch size is set to 2000, what a particular stage does, which environment a URL points to, and what happens if you change a value. Every example config in Lucille is heavily commented.

```hocon
# maximum time allowed between kafka polls before consumer is evicted from consumer group
maxPollIntervalSecs: 600 # 10 minutes
```

### File Includes

A single monolithic config file becomes unwieldy for organizations running multiple ingests. HOCON's `include` directive lets you compose configs from smaller, reusable pieces:

```hocon
# main config includes connector definitions from separate files
csv_connector = { include "csv-connector.conf" }
json_connector = { include "json-connector.conf" }

connectors: [${csv_connector}, ${json_connector}]

# include pipeline definitions from another file
include "simple-pipeline.conf"
```

This means:
- Connector definitions can be shared across multiple ingests.
- Pipeline definitions can be maintained independently.
- Connection strings for shared infrastructure live in one file and are included everywhere.
- Changes to a shared component propagate to all ingests that include it.

### Variable Substitution and Reuse

HOCON supports internal references, so a value defined once can be reused:

```hocon
common {
  batchSize: 1000
  timeout: 5000
}

indexer {
  batchSize: ${common.batchSize}
  batchTimeout: ${common.timeout}
}
```

Combined with environment variable substitution (`${?ENV_VAR}`), this eliminates repetition and ensures consistency.

### Relaxed Syntax

HOCON allows omitting quotes around keys, using `=` or `:` for assignment, and trailing commas. This makes configs more readable and less error-prone to edit by hand:

```hocon
# HOCON — clean and readable
opensearch {
  url: "https://localhost:9200"
  index: enron
  acceptInvalidCert: true
}

# equivalent JSON — noisier
{
  "opensearch": {
    "url": "https://localhost:9200",
    "index": "enron",
    "acceptInvalidCert": true
  }
}
```

### List Concatenation

HOCON supports appending to lists, which is useful for extending configs:

```hocon
pipelines: [{name: "pipeline1", stages: [...]}]
pipelines: ${pipelines} [{name: "pipeline2", stages: [...]}]
```

---

## The Typesafe Config Library

Lucille uses the [Typesafe Config](https://github.com/lightbend/config) library (now maintained under `com.typesafe:config`). This library provides:

**Automatic resolution order.** When you call `ConfigFactory.load()`, the library merges configuration from multiple sources in a defined precedence:
1. System properties (`-Dproperty=value`) — highest priority
2. `application.conf` on the classpath (or the file specified by `-Dconfig.file=...`)
3. `reference.conf` on the classpath — lowest priority (used for defaults)

This means a Lucille deployment can override any config value via a system property without modifying the config file — useful for one-off test runs or CI overrides.

**The `-Dconfig.file` system property.** This is how Lucille selects which config file to use at runtime:

```bash
java -Dconfig.file=conf/my-ingest.conf -cp 'target/lib/*' com.kmwllc.lucille.core.Runner
```

In a Docker container, this becomes:

```dockerfile
ENV CONF=""
ENTRYPOINT java -Dconfig.file=${CONF} -cp 'target/lib/*' com.kmwllc.lucille.core.Runner
```

The container is launched with `--env CONF=conf/my-ingest.conf`, keeping the image generic and the config external.

**Typed access.** The library provides `getString()`, `getInt()`, `getBoolean()`, `getStringList()`, `getConfig()` (for nested blocks), and `getConfigList()` (for lists of objects like connectors and stages). Type errors are caught at access time with clear error messages.

**Path expressions.** Nested values are accessed with dot-separated paths: `config.getString("opensearch.url")`. This maps naturally to the nested block structure of HOCON.

---

## Configuration Patterns in Lucille

### The Override Pattern for Environment-Specific Values

The most common pattern in Lucille configs is declaring a default and then overriding it with an optional environment variable:

```hocon
opensearch {
  url: "http://localhost:9200"
  url: ${?OPENSEARCH_URL}

  index: "my-index"
  index: ${?OPENSEARCH_INDEX}
}
```

In HOCON, later assignments to the same key override earlier ones. The `${?...}` syntax (with the `?`) means the substitution is optional — if the env var is not set, the override line is silently ignored and the default stands. Without the `?`, a missing env var would cause a config resolution error.

This pattern appears throughout Lucille's example configs for URLs, credentials, file paths, and index names.

### Composing Configs from Multiple Files

The file-to-file example demonstrates how to split a config into reusable pieces:

```hocon
# file-to-file-example.conf (main config)

# include connector definitions from separate files and bind to variables
csv_connector = { include "csv-connector.conf" }
json_connector = { include "json-connector.conf" }

# reference the included connectors in the connector list
connectors: [${csv_connector}, ${json_connector}]

# include pipeline definitions from another file
include "simple-pipeline.conf"
```

```hocon
# csv-connector.conf (reusable connector definition)
class: "com.kmwllc.lucille.connector.FileConnector"
name: "csv_connector"
paths: ["conf/source.csv"]
pipeline: "simple_pipeline"
fileHandlers: {
  csv: { }
}
```

```hocon
# simple-pipeline.conf (reusable pipeline definition)
pipelines: [
  {
    name: "simple_pipeline"
    stages: [
      {
        class: "com.kmwllc.lucille.stage.RenameFields"
        fieldMapping {
          "name" : "my_name"
          "price" : "my_price"
          "country" : "my_country"
        }
      }
    ]
  }
]
```

This decomposition means:
- The connector definition can be reused in other ingests.
- The pipeline definition can be shared across connectors.
- The main config is a short composition of included pieces.

### Containerized Deployment

The file-to-file example includes a Dockerfile that demonstrates the standard pattern for running Lucille in a container:

```dockerfile
FROM eclipse-temurin:17
COPY target/ /target/
COPY conf/ /conf/
ENV CONF=""
ENTRYPOINT java -Dconfig.file=${CONF} -cp 'target/lib/*' com.kmwllc.lucille.core.Runner
```

The container is launched with:
```bash
docker run --env CONF=conf/my-ingest.conf \
           --env OPENSEARCH_URL=https://prod-cluster:9200 \
           --env OPENSEARCH_INDEX=production \
           -it lucille-example
```

The config file provides structure and defaults. Environment variables provide deployment-specific overrides. No code changes, no config file modifications, no rebuild required.

---

## Pre-Run Validation

Before any run starts, Lucille validates the entire configuration against every component's SPEC declaration. This catches:

- Missing required parameters
- Unrecognized parameters (typos)
- Type mismatches
- Invalid combinations

All errors are reported at once — not fail-fast on the first error. This means a developer can fix all config issues in one pass rather than discovering them one at a time through repeated failed starts.

Configuration can also be validated without execution using the `-validate` flag:

```bash
java -Dconfig.file=conf/my-ingest.conf -cp 'target/lib/*' com.kmwllc.lucille.core.Runner -validate
```

This is useful in CI pipelines that want to catch config errors before deployment without actually running an ingest.

The `-render` flag prints the fully resolved config (with all substitutions applied) so you can verify what values Lucille will actually see at runtime:

```bash
java -Dconfig.file=conf/my-ingest.conf -cp 'target/lib/*' com.kmwllc.lucille.core.Runner -render
```

---

## How Components Read Configuration

Every Lucille component receives a `Config` object and reads its parameters using the Typesafe Config API:

```java
// Required parameters — throw if absent
String url = config.getString("opensearch.url");
int batchSize = config.getInt("indexer.batchSize");
List<String> fields = config.getStringList("columns");

// Optional parameters with defaults — use ConfigUtils.getOrDefault
String dest = ConfigUtils.getOrDefault(config, "dest", "output");
int timeout = ConfigUtils.getOrDefault(config, "batchTimeout", 100);
boolean flag = ConfigUtils.getOrDefault(config, "acceptInvalidCert", false);

// Check presence before reading
if (config.hasPath("s3")) {
  Config s3Config = config.getConfig("s3");
  String accessKey = s3Config.getString("accessKeyId");
}

// Nested config blocks
List<? extends Config> stages = config.getConfigList("stages");
```

The `ConfigUtils.getOrDefault` utility is Lucille's standard pattern for optional parameters. It avoids the verbosity of `config.hasPath("x") ? config.getString("x") : default` that would otherwise appear in every component.

---

## Configuration Structure

A complete Lucille config has the following top-level structure:

```hocon
connectors: [...]     # list of connector definitions (executed in sequence)
pipelines: [...]      # list of pipeline definitions (referenced by connectors)
indexer { ... }       # indexer type and general settings
solr { ... }          # or opensearch { ... } or elastic { ... } — backend-specific settings
worker { ... }        # worker thread count, timeout, retry settings
kafka { ... }         # Kafka connection info (for distributed mode)
publisher { ... }     # backpressure settings
runner { ... }        # runner-level settings (connector timeout, metrics logging)
log { ... }           # logging interval
zookeeper { ... }     # ZooKeeper connection (for distributed retry tracking)
```

Each connector references a pipeline by name. Each pipeline defines its stages. The indexer block is shared across all pipelines (a single destination for the run). This structure means you can define multiple connectors feeding different pipelines, all writing to the same search backend, in a single config file.

---

## Summary

Lucille's configuration approach is built on three principles:

1. **Delegate complexity to the config library.** Environment variable resolution, file composition, type coercion, and precedence rules are handled by Typesafe Config, not by application code. Components simply call typed getters and get resolved values.

2. **Validate early and completely.** The SPEC system catches config errors before any work begins. All errors are reported at once. Validation can run without execution for CI integration.

3. **Support real-world deployment patterns.** File includes for config reuse across ingests. Environment variable substitution for containerized deployments. The `-Dconfig.file` system property for selecting configs at runtime. The `-render` flag for debugging resolved values.

The choice of HOCON over JSON is a pragmatic one: comments, includes, variable substitution, and relaxed syntax are not luxuries in a config file that defines an entire ingestion pipeline — they are necessities that eliminate repetition, enable reuse, and make configs maintainable as systems grow.
