---
title: Getting Started
weight: 2
description: >
 Understanding the basics to quickly get started.
---

## Installation

See the [installation guide]({{< relref "docs/getting-started/installation" >}}) to install prerequisites, clone the repository, and build Lucille.

## Try it out

Lucille includes a few examples in the `lucille-examples` module to help you get started.

To see how to ingest the contents of a local CSV file into an instance of Apache Solr, refer to the [simple-csv-solr-example](https://github.com/kmwtechnology/lucille/tree/main/lucille-examples/lucille-simple-csv-solr-example).

To run this example, start an instance of Apache Solr on port 8983 and create a collection called `quickstart`. For more information about how to use Solr, see the [Apache Solr Reference Guide](https://solr.apache.org/guide/solr/latest/getting-started/introduction.html)).

Go to `lucille-examples/lucille-simple-csv-solr-example` in your working copy of Lucille and run:

`mvn clean install`

`./scripts/run_ingest.sh`

This script executes Lucille with a configuration file named `simple-csv-solr-example.conf` that tells Lucille to read a CSV of top songs and send each row as a document to Solr.

Run a commit with `openSearcher=true` on your `quickstart` collection to make the documents visible. Go to your Solr admin dashboard, execute a `*:*` query and you should see the songs from the source file now visible as Solr documents.

## Quick Start Guide - Local Mode

**Scope:** The steps below run Lucille from a **source build** (built locally with Maven).

### What is Local Mode?

Local mode runs all Lucille components (connector, pipeline, and indexer) inside a single JVM process that you start locally. Your configuration may still interact with external systems (e.g., S3, Solr, OpenSearch/Elasticsearch), but the Lucille runtime itself executes entirely within that single JVM.

### Prepare a Configuration File

You'll run Lucille by pointing it at a config file that declares your **connectors**, **pipelines**, and **indexers**. See the [configuration]({{< relref "docs/architecture/components/config" >}}) docs for the full schema and supported components.

### Run Lucille Locally

From the repository root, run the Runner with your config file:

```bash
java \
  -Dconfig.file=<PATH/TO/YOUR/CONFIG.conf> \
  -cp 'lucille-core/target/lucille.jar:lucille-core/target/lib/*' \
  com.kmwllc.lucille.core.Runner
```

**What this Does**
* `-Dconfig.file=<PATH/TO/YOUR/CONFIG.conf>` tells Lucille where to find your configuration.
* `-cp 'lucille-core/target/lucille.jar:lucille-core/target/lib/*'` loads Lucille and its dependencies.
* `com.kmwllc.lucille.core.Runner` boots the Lucille engine in local mode and runs the configured pipeline to completion.

**Trouble Running Lucille?**

See the [troubleshooting]({{< relref "docs/prod/troubleshooting" >}}) guide for common pitfalls.

## Quick Start Guide - Distributed Mode

### What is Distributed Mode?

Distributed mode allows you to scale Lucille to take advantage of available hardware by running each Lucille component in its own JVM and using Kafka for document transport and event tracking. You start:
* A **Runner** (Publisher + Connectors) to publish documents onto Kafka.
* One or more **Workers** to process the documents through a pipeline.
* An **Indexer** to write the processed documents to your destination (Solr, OpenSearch, Elasticsearch, CSV, etc.).

This guide assumes Kafka and your destination system are already running and reachable. This guide focuses on running Lucille itself. For details on configuration structure and component options, see the corresponding docs.

### Prepare a Configuration File

You'll run Lucille by pointing it at a config file that declares your pipeline. See the [configuration]({{< relref "docs/architecture/components/config" >}}) docs for the full schema and supported components.

Use a single config that defines: your **connector(s)**, your **pipeline(s)**, **kafka** configuration, and your **indexer** and its backend config (e.g., `solr {}`, `opensearch {}`, etc).

### Start Components (Separate JVMs)

#### A) Start the Runner (publishes to Kafka)

The runner publishes documents to the Kafka source topic, listens for pipeline run events, logs run statistics, and waits for the run to complete.

```bash
java \
 -Dconfig.file=<PATH/TO/YOUR/CONFIG.conf> \
 -cp 'lucille-core/target/lucille.jar:lucille-core/target/lib/*' \
 com.kmwllc.lucille.core.Runner \
 -useKafka
```

#### B) Start one or more Workers

Each worker consumes documents from the Kafka source topic, processes each document through the configured pipeline, and writes the processed documents to the Kafka destination topic.

```bash
java \
 -Dconfig.file=<PATH/TO/YOUR/CONFIG.conf> \
 -cp 'lucille-core/target/lucille.jar:lucille-core/target/lib/*' \
 com.kmwllc.lucille.core.Worker \
 simple_pipeline
```

#### C) Start the Indexer

The indexer consumes documents from the Kafka destination topic and sends batches of processed documents to the configured search backend.

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
* `com.kmwllc.lucille.core.Runner -useKafka` starts the run and interacts with Kafka as described above.
* `com.kmwllc.lucille.core.Worker <pipelineName>` processes documents through the configured pipeline as described above.
* `com.kmwllc.lucille.core.Indexer <pipelineName>` writes processed documents to the configured backend as described above.

**Trouble Running Lucille?**

See the [troubleshooting]({{< relref "docs/prod/troubleshooting" >}}) guide for common pitfalls.

## Verifying Your Lucille Run

* **Logs:** You should see Lucille start up, load your configuration, report component initialization, record counts, and completion status.

    During the run, you will see throughput and latency metrics like:
    
    ```
    25/10/31 13:40:21 6790d2e9-1079-4f15-b75a-acab4ae8e4c2  INFO WorkerPool: 27017 docs processed. One minute rate: 1787.10 docs/sec. Mean pipeline latency: 10.63 ms/doc.
    25/10/31 13:40:22 6790d2e9-1079-4f15-b75a-acab4ae8e4c2  INFO PublisherImpl: 37029 docs published. One minute rate: 3225.69 docs/sec. Mean connector latency: 0.00 ms/doc. Waiting on 21014 docs.
    25/10/31 13:40:22 6790d2e9-1079-4f15-b75a-acab4ae8e4c2  INFO Indexer: 17016 docs indexed. One minute rate: 455.07 docs/sec. Mean backend latency: 6.90 ms/doc.
    ```

    At completion, Lucille prints a stage-by-stage performance summary and a final run result:
    
    ```
    25/10/31 13:46:47 6790d2e9-1079-4f15-b75a-acab4ae8e4c2  INFO Stage: Stage test_source metrics. Docs processed: 200000. Mean latency: 0.0003 ms/doc. Children: 0. Errors: 0.
    25/10/31 13:46:47 6790d2e9-1079-4f15-b75a-acab4ae8e4c2  INFO Stage: Stage test_summary metrics. Docs processed: 200000. Mean latency: 0.3532 ms/doc. Children: 0. Errors: 0.
    25/10/31 13:46:47 6790d2e9-1079-4f15-b75a-acab4ae8e4c2  INFO Runner: 
    RUN SUMMARY: Success. 1/1 connectors complete. All published docs succeeded.
    connector1: complete. 200000 docs succeeded. 0 docs failed. 0 docs dropped. Time: 416.47 secs.
    25/10/31 13:46:47 6790d2e9-1079-4f15-b75a-acab4ae8e4c2  INFO Runner: Run took 417.46 secs.
    ```

* **Output:** View your target service (e.g., Elasticsearch) to verify your index.