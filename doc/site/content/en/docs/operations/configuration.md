---
title: "Configuration Management"
weight: 1
date: 2025-06-09
description: >
  Environment variable substitution, config composition patterns, containerized deployment, distributed mode config, and pre-run validation.
---

This page covers the operational patterns for working with Lucille's configuration system — how to use environment variable substitution, how to compose configs from reusable files, how to deploy configs in containers, and how to validate configs before running. For the architectural rationale behind Lucille's choice of HOCON and the Typesafe Config library, see [Architecture: Config]({{< relref "docs/architecture/components/Config" >}}).

---

## Environment Variable Substitution

In containerized deployments (Docker, Kubernetes), credentials and environment-specific settings are typically provided via container environment variables or mounted secrets. A database password, an API key, a search engine URL — these should not be hardcoded in a config file that lives in version control.

Lucille handles this through HOCON's substitution syntax:

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

The [lucille-file-to-file-example](https://github.com/kmwtechnology/lucille/tree/main/lucille-examples/lucille-file-to-file-example) demonstrates how to split a config into reusable pieces:

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

The [lucille-file-to-file-example](https://github.com/kmwtechnology/lucille/tree/main/lucille-examples/lucille-file-to-file-example) includes a Dockerfile that demonstrates the standard pattern for running Lucille in a container:

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

For a complete, annotated listing of every supported top-level configuration property (excluding per-stage parameters), see [`application-example.conf`](https://github.com/kmwtechnology/lucille/blob/main/application-example.conf) in the repository root. It covers all valid keys for `indexer`, `worker`, `kafka`, `publisher`, `runner`, `log`, `zookeeper`, and the other top-level blocks, with comments explaining each option.

---

## Configuration in Distributed Mode

A configuration file is necessary for running all Lucille components — the full system in local mode as well as dedicated Workers, Indexers, and WorkerIndexers in distributed and streaming modes.

### One Config for All Components

In distributed mode, it is a best practice to pass the same full configuration file to all components, even though each component only reads the sections relevant to it. A Worker, for example, does not need to know about indexing settings, and an Indexer does not need to know the pipeline definition. But passing the same file to all components is simpler and less error-prone than maintaining separate configs for each component type.

Users are responsible for ensuring the same config is provided to all components. Lucille does not detect inconsistencies across processes. If a user mistakenly passes two different configs — with different definitions of the same named pipeline — to two Worker instances, the Workers will apply different enrichment logic to documents. The result would be inconsistent enrichment in the search index, and Lucille would not raise an error because each Worker sees only its own config and has no way to compare it with another process.

### Why This Is Not a Problem in Practice

In practice, config consistency does not turn out to be a significant operational challenge. When setting up a distributed ingest, you already need a way to copy the same Lucille JARs and libraries to all machines (or containers) where Lucille will run. The configuration files for the project should be copied at the same time, using the same mechanism.

In a containerized deployment this is trivial: include the config file in your Docker image and then use the same image for launching all Lucille components (Runner, Worker, Indexer, WorkerIndexer). The image guarantees that every component sees identical JARs, libraries, and configuration. The only variation between containers is the entrypoint command that determines which component role to start.

---

## Summary

Lucille's configuration approach is built on three principles:

1. **Delegate complexity to the config library.** Environment variable resolution, file composition, type coercion, and precedence rules are handled by Typesafe Config, not by application code. Components simply call typed getters and get resolved values.

2. **Validate early and completely.** The SPEC system catches config errors before any work begins. All errors are reported at once. Validation can run without execution for CI integration.

3. **Support real-world deployment patterns.** File includes for config reuse across ingests. Environment variable substitution for containerized deployments. The `-Dconfig.file` system property for selecting configs at runtime. The `-render` flag for debugging resolved values.
