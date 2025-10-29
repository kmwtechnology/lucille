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


## Quick Start Guide - Local Mode

### What is Local Mode?

Local mode runs all Lucille components (connector, pipeline, and indexer) inside a single JVM process that you start locally. Your configuration may still interact with external systems (e.g., S3, Solr, OpenSearch/Elasticsearch), but the Lucille runtime itself executes entirely within that single JVM.

This guide focuses on running Lucille itself. For details on configuration structure and component options, see the corresponding docs.

### Prerequisites

* Java 17+
* Maven
* Git

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

### 3) Run Lucille Locally

From the repository root, navigate to the core module's build output and run the Runner with your config file:

```bash
cd lucille-core/target

java \
  -Dconfig.file=<PATH/TO/YOUR/CONFIG.conf> \
  -cp 'lucille.jar:lib/*' \
  com.kmwllc.lucille.core.Runner
```

**What this Does**
* `-Dconfig.file=<PATH/TO/YOUR/CONFIG.conf>` tells Lucille where to find your configuration.
* `-cp 'lucille.jar:lib/*'` loads Lucille and its dependencies.
* `com.kmwllc.lucille.core.Runner` boots the Lucille engine in local mode and runs the configured pipeline to completion.

### 4) Verify the Run

* **Logs:** You should see Lucille start up, load your configuration, report component initialization, record counts, and completion status.
* **Output:** View your target service (e.g., Elasticsearch) to verify your index.
### Troubleshooting

* **Java/Maven not found:** Confirm `java -version` is 17+ and `mvn -v` is available in your PATH.
* **ClassPath issues:** Ensure you are in `lucille-core/target` when running the command and that `lucille.jar` and `lib/` exist after the Maven build.
* **Config parsing errors:** Double-check your config path and syntax. Consult the config docs for valid fields and component names.

