---
title: Support Matrix
weight: 7
date: 2025-06-09
description: Supported Java versions, search backends, Kafka versions, and operating systems.
---

## Java

| Version | Status |
|---|---|
| Java 17 | Supported (minimum required) |
| Java 21 | Supported |

Lucille is compiled targeting Java 17. Both versions are tested in CI via GitHub Actions using Eclipse Temurin distributions.

## Build Tool

Apache Maven 3.x is required to build Lucille from source.

## Search Backends

The following backends are supported via the Indexer component.

| Backend | Tested Version | Notes |
|---|---|---|
| Apache Solr | 9.x | Client: `solr-solrj` 9.8.0 |
| OpenSearch | 2.x | Client: `opensearch-java` 2.11.1 |
| Elasticsearch | 8.x | Client: `elasticsearch-java` 8.18.4 |
| CSV | — | Writes indexed documents to a local CSV file; no server required |
| Nop | — | Discards output; used for testing and dry runs |

## Kafka (Distributed Mode)

| Component | Version |
|---|---|
| kafka-clients | 4.0.0 |
| Kafka broker | 3.x, 4.x |

Kafka is only required when running in distributed mode (`-usekafka`). Local mode uses in-memory queues and has no Kafka dependency.

## Operating Systems

| OS | Status |
|---|---|
| Linux | Supported; used in production deployments |
| macOS | Supported; tested in CI |
| Windows | Not officially tested |

## License

Apache License, Version 2.0. See [LICENSE](https://github.com/kmwtechnology/lucille/blob/main/LICENSE) in the repository.
