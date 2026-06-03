---
title: REST API
weight: 7
date: 2025-06-09
description: >
  HTTP REST API for managing Lucille configs and triggering runs without the CLI.
aliases:
  - /docs/reference/plugins/
---

The `lucille-api` plugin adds an HTTP REST API built on [Dropwizard](https://www.dropwizard.io/) that allows managing configs and triggering runs over HTTP rather than via the CLI. It includes a Swagger UI and optional basic authentication.

**Current limitation:** The REST API launches all runs in local mode (in-memory queues, Workers and Indexer as threads within the API server's JVM). It cannot currently trigger a distributed Kafka-based ingest. If you need distributed mode, use the CLI Runner with `-usekafka`.

**Maven dependency:**
```xml
<dependency>
  <groupId>com.kmwllc</groupId>
  <artifactId>lucille-api</artifactId>
  <version>${lucille.version}</version>
</dependency>
```

---

## Starting the API Server

The API uses a Dropwizard YAML configuration file. An example config is provided at [`lucille-plugins/lucille-api/conf/api.yml`](https://github.com/kmwtechnology/lucille/blob/main/lucille-plugins/lucille-api/conf/api.yml).

```bash
java \
  -cp 'lucille-plugins/lucille-api/target/lucille-api-{version}.jar:lucille-plugins/lucille-api/target/lib/*' \
  com.kmwllc.lucille.APIApplication server lucille-plugins/lucille-api/conf/api.yml
```

The server listens on port `8080` by default. Swagger UI is available at `http://localhost:8080/swagger`.

---

## Endpoints

All endpoints are under the `/v1` prefix.

### Config Management

| Method | Path | Description |
|---|---|---|
| `POST` | `/v1/config` | Submit a config as a JSON object. Returns a `configId` UUID. |
| `GET` | `/v1/config` | List all stored configs. |
| `GET` | `/v1/config/{configId}` | Retrieve a specific config by ID. |

### Run Management

| Method | Path | Description |
|---|---|---|
| `POST` | `/v1/run` | Start a run. Request body: `{"configId": "<uuid>"}`. Returns `RunDetails`. |
| `GET` | `/v1/run` | List all runs and their status. |
| `GET` | `/v1/run/{runId}` | Get details for a specific run. |

### Health and Observability

| Method | Path | Description |
|---|---|---|
| `GET` | `/v1/livez` | Liveness check — returns 200 if the service is running. |
| `GET` | `/v1/readyz` | Readiness check — returns 200 if the service is ready. |
| `GET` | `/v1/systemstats` | CPU, RAM, JVM heap, and disk usage as JSON. |
| `GET` | `/v1/systemstats/metrics` | Dropwizard Codahale metrics registry as JSON. |

---

## Typical Workflow

1. `POST /v1/config` with your HOCON config as JSON — receive a `configId`.
2. `POST /v1/run` with `{"configId": "<uuid>"}` — receive a `runId`.
3. `GET /v1/run/{runId}` — poll for run status until complete.

---

## Configuring the API

It is important to draw a distinction between configuration for the Lucille API and configuration for Lucille (a Lucille run).
Configuration for the Lucille API takes the form of a `.yml` file and controls an instance of the API. Within the API,
you'll upload your Lucille Configs (as JSON) to ultimately run them.

### Preset Configs

You can update your Lucille API configuration to define a directory containing preset Configs you would like to be loaded
upon initialization of the Lucille API. 

```yaml
presetConfig:
  configDirectoryPath: /path/to/my/configs
```

The provided path **must** be a directory. Only `.json` and `.conf` files in this directory will be considered. Instead of a UUID,
configs loaded this way are keyed by their filename, including their file extension (e.g. `config1.conf`).