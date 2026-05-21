---
title: Docker Compose
weight: 5
date: 2025-06-09
description: Running Lucille in distributed mode using Docker Compose.
---

## Docker Compose Deployment

Docker Compose is the simplest way to run Lucille in distributed mode. The [lucille-distributed-example](https://github.com/kmwtechnology/lucille/tree/main/lucille-examples/lucille-distributed-example) provides a complete working template. The key structure:

```yaml
services:
  zookeeper:
    # Bundled with Kafka; required for Kafka's internal coordination
    image: ...

  kafka:
    depends_on: [zookeeper]
    healthcheck:
      test: ["CMD", "kafka-topics.sh", "--bootstrap-server=kafka:9092", "--list"]

  worker:
    depends_on:
      kafka: { condition: service_healthy }
    entrypoint: java -Dconfig.file=/conf/config.conf -cp '/target/lib/*'
                     com.kmwllc.lucille.core.Worker my-pipeline

  indexer:
    depends_on:
      kafka: { condition: service_healthy }
      solr:  { condition: service_healthy }
    healthcheck:
      # Delegate healthcheck to the search backend — Runner waits for this before starting
      test: curl -f 'http://solr:8983/solr/...'
    entrypoint: |
      # Create the collection, then start the Indexer
      curl 'http://solr:8983/solr/admin/collections?action=CREATE&name=quickstart...'
      java -Dconfig.file=/conf/config.conf -cp '/target/lib/*'
           com.kmwllc.lucille.core.Indexer my-pipeline

  runner:
    depends_on:
      kafka:   { condition: service_healthy }
      indexer: { condition: service_healthy }  # Ensures Indexer + backend are ready
    entrypoint: java -Dconfig.file=/conf/config.conf -cp '/target/lib/*'
                     com.kmwllc.lucille.core.Runner -usekafka
```

Key design points from the example:
- **All components share one config file** — `config.file` points to the same `.conf` for all containers.
- **Classpath is `target/lib/*`** — Maven's `dependency:copy-dependencies` puts all JARs there; the main lucille JAR and all dependencies are sibling files.
- **The Indexer container's health check proxies the search backend's health** — this causes Docker Compose to hold the Runner until the search backend is ready, without the Runner needing to know anything about it.
- **Workers start before the Runner** — Kafka consumer group rebalancing takes time; workers should be consuming before the Runner begins publishing documents.

