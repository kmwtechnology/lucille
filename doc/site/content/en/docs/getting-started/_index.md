---
title: Getting Started
weight: 2
description: >
 Understanding the basics to quickly get started.
---

## Prerequisites
* Java development environment with Java 17 or later
* Recent Maven version

## Installation
Start by cloning the repository:

`git clone https://github.com/kmwtechnology/lucille.git`

At the top level of the project, run:

`mvn clean install`


## Try it out
Lucille includes a few examples in the `lucille-examples` module to help you get started.

To see how to ingest the contents of a local CSV file into an instance of Apache Solr, refer to `lucille-examples/simple-csv-solr-example`.

To run this example, start an instance of Apache Solr on port 8983 and create a collection called `quickstart`. For more information about how to use Solr, see the [Apache Solr Reference Guide](https://solr.apache.org/guide/solr/latest/getting-started/introduction.html)).

Go to `lucille-examples/lucille-simple-csv-solr-example` in your working copy of Lucille and run:

`mvn clean install`

`./scripts/run_ingest.sh`

This script executes Lucille with a configuration file named `simple-csv-solr-example.conf` that tells Lucille to read a CSV of top songs and send each row as a document to Solr.

Run a commit with `openSearcher=true` on your `quickstart` collection to make the documents visible. Go to your Solr admin dashboard, execute a `*:*` query and you should see the songs from the source file now visible as Solr documents.


## Quick Start Guide - Distributed Mode

### What is Distributed Mode?

Distributed mode runs each Lucille component in its own JVM and uses Kafka for document transport and event tracking. You start:
* A **Runner** (Publisher + Connectors) to publish documents onto Kafka.
* One or more **Workers** to process the documents through a pipeline.
* An **Indexer** to write the processed documents to your destination (Solr, OpenSearch, Elasticsearch, CSV, etc.).

This guide assumes Kafka and your destination system are already running and reachable. This guide focuses on running Lucille itself. For details on configuration structure and component options, see the corresponding docs.

### 1) Clone and Build

```bash
# clone
git clone https://github.com/kmwtechnology/lucille.git
cd lucille

# build all modules
mvn clean install
```

This compiles the modules and produces build artifacts under each module's target directory.

### 2) Prepare a Configuration File

You'll run Lucille by pointing it at a config file that declares your pipeline. See the configuration docs for the full schema and supported components.

Use a single config that defines: your **connector(s)**, your **pipeline(s)**, **kafka** configuration, and your **indexer** and its backend config (e.g., `solr {}`, `opensearch {}`, etc).

### 3) Start Components (Separate JVMs)

#### A) Start the Runner (publishes to Kafka)

```bash
java \
 -Dconfig.file=<PATH/TO/YOUR/CONFIG.conf> \
 -cp 'lucille-core/target/lucille.jar:lucille-core/target/lib/*' \
 com.kmwllc.lucille.core.Runner \
 -useKafka
```

#### B) Start one or more Workers

```bash
java \
 -Dconfig.file=<PATH/TO/YOUR/CONFIG.conf> \
 -cp 'lucille-core/target/lucille.jar:lucille-core/target/lib/*' \
 com.kmwllc.lucille.core.Worker \
 simple_pipeline
```

#### C) Start the Indexer

```bash
java \
 -Dconfig.file=<PATH/TO/YOUR/CONFIG.conf> \
 -cp 'lucille-core/target/lucille.jar:lucille-core/target/lib/*' \
 com.kmwllc.lucille.core.Indexer \
 simple_pipeline
```

**What this Does**

* `-Dconfig.file=<PATH/TO/YOUR/CONFIG.conf>` tells Lucille where to find your configuration.
* `-cp 'lucille-core/target/lucille.jar:lucille-core/target/lib/*'` loads Lucille and its dependencies.
* `com.kmwllc.lucille.core.Runner -usekafka` starts the Publisher and Connectors and publishes documents to Kafka topics.
* `com.kmwllc.lucille.core.Worker <pipelineName>` polls documents from Kafka, executes the configured pipeline, and forwards them for indexing.
* `com.kmwllc.lucille.core.Indexer <pipelineName>` consumes processed documents from Kafka, batches them, and writes to your destination.

### 4) Verify the Run

- **Logs:** Each process should show startup and processing output.
- **Output:** View your target service (e.g., Elasticsearch) to verify your index.

### Troubleshooting

* **Java/Maven not found:** Confirm `java -version` is 17+ and `mvn -v` is available in your PATH. Ensure that `JAVA_HOME` is set to a JDK 17+ installation.
* **Classpath issues:** Ensure `lucille-core/target/lucille.jar` and `lucille-core/target/lib/` exist after the Maven build. Avoid running *from inside* `target/`, since `mvn clean` removes it and can cause issues.
* **Kafka connectivity:** Check `kafka.bootstrapServers` and any client property files if used.
* **Indexer connection issues:** Ensure destination config (Solr/OpenSearch/Elasticsearch/CSV) matches a reachable backend.
* **Config parsing errors:** Double-check your config path and syntax. Consult the config docs for valid fields and component names.
