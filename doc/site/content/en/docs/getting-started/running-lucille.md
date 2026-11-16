---
title: Running Lucille
weight: 5
date: 2025-06-09
description: >
  How to run Lucille in local and distributed mode, with examples and guidance on verifying a run.
---

## Try the Examples

Lucille's `lucille-examples` module contains several runnable examples:

| Example | Description |
|---|---|
| [lucille-simple-csv-solr-example](https://github.com/kmwtechnology/lucille/tree/main/lucille-examples/lucille-simple-csv-solr-example) | Ingest a CSV file into Solr in local mode. A good first example. |
| [lucille-distributed-example](https://github.com/kmwtechnology/lucille/tree/main/lucille-examples/lucille-distributed-example) | Full distributed mode with Runner, Worker, Indexer, Kafka, ZooKeeper, and Solr each running in their own Docker container via Docker Compose. Includes an integration test that verifies the ingest result. Run with `mvn verify -Pnightly` from that directory. |
| [lucille-api-example](https://github.com/kmwtechnology/lucille/tree/main/lucille-examples/lucille-api-example) | Runs the Lucille API Docker image via Docker Compose, then uploads two configs and runs them concurrently through the API. Includes an integration test that verifies both runs' output. Build with `mvn clean install -DskipTests -Pshaded-plugin-jars` from the top level, then run with `mvn verify -Pnightly` from that directory. |

To run the simple CSV example, start an instance of Apache Solr on port 8983 and create a collection called `quickstart`. Then from `lucille-examples/lucille-simple-csv-solr-example`, run:

```bash
mvn clean install
./scripts/run_ingest.sh
```

This executes Lucille with `simple-csv-solr-example.conf`, which reads a CSV of top songs and sends each row as a document to Solr. After the run, issue a commit with `openSearcher=true` on your `quickstart` collection and run a `*:*` query in the Solr admin dashboard to see the indexed documents.

---

## Local Mode

Local mode runs all Lucille components — Connector, Workers, and Indexer — as threads inside a single JVM. No Kafka or external messaging is required. This is the right mode for development, testing, and single-machine production runs.

### Prepare a Configuration File

Point Lucille at a config file declaring your **connectors**, **pipelines**, and **indexer**. See [Configuration Management]({{< relref "docs/operations/configuration" >}}) for the full schema, or follow [Your First Pipeline]({{< relref "docs/getting-started/your-first-pipeline" >}}) for a step-by-step walkthrough.

### Run the Runner

From the repository root, after `mvn clean install`:

```bash
java \
  -Dlog4j2.configurationFile=classpath:lucille-log4j2.xml \
  -Dconfig.file=<PATH/TO/YOUR/CONFIG.conf> \
  -cp 'lucille-all/target/lib/*' \
  com.kmwllc.lucille.core.Runner
```

| Flag | What it does |
|---|---|
| `-Dlog4j2.configurationFile=...` | Selects the Lucille logging config bundled in `lucille-logging`. Without it, Log4j2 logs only errors |
| `-Dconfig.file=...` | Path to your HOCON configuration file |
| `-cp '...'` | `lucille-all/target/lib` holds lucille-core, lucille-logging, every plugin module (Solr, OpenSearch, Elasticsearch, Tika, and so on), and all their runtime dependencies |
| `com.kmwllc.lucille.core.Runner` | Boots the Lucille engine in local mode and runs the configured pipeline to completion |

If your config uses only components in lucille-core, a smaller classpath is enough: `'lucille-core/target/lucille.jar:lucille-core/target/lib/*:lucille-logging/target/lucille-logging.jar:lucille-logging/target/lib/*'`. Leaving out `lucille-logging` gives `No SLF4J providers were found` and no log output.

Alternatively, run the published Docker image. `kmwtechnology/lucille-all` contains every plugin; `kmwtechnology/lucille` contains only core and logging:

```bash
docker run --rm -v "$PWD":/lucille/work -w /lucille/work \
  -e LUCILLE_CONF=/lucille/work/<YOUR-CONFIG.conf> \
  kmwtechnology/lucille-all:1.0.0-alpha.0
```

See the [troubleshooting guide]({{< relref "docs/operations/troubleshooting" >}}) if Lucille doesn't start as expected.

---

## Distributed Mode

Distributed mode runs each Lucille component as its own JVM process, communicating through Apache Kafka. Use this when you need to scale out across multiple machines or processes.

This guide assumes Kafka and your destination system are already running and reachable. For production deployment patterns including Docker Compose and Kubernetes, see [Production Deployment]({{< relref "docs/operations/deployment" >}}).

### Prepare a Configuration File

Use a single config file shared by all components. It must declare your **connector(s)**, **pipeline(s)**, **kafka** configuration, and your **indexer** with its backend config.

### Start the Runner

The Runner publishes documents to the Kafka source topic, listens for pipeline run events, logs run statistics, and waits for the run to complete:

```bash
java \
  -Dlog4j2.configurationFile=classpath:lucille-log4j2.xml \
  -Dconfig.file=<PATH/TO/YOUR/CONFIG.conf> \
  -cp 'lucille-all/target/lib/*' \
  com.kmwllc.lucille.core.Runner \
  -distributed
```

### Start Workers

Each Worker consumes documents from the Kafka source topic, processes each document through the configured pipeline, and writes processed documents to the Kafka destination topic:

```bash
java \
  -Dlog4j2.configurationFile=classpath:lucille-log4j2.xml \
  -Dconfig.file=<PATH/TO/YOUR/CONFIG.conf> \
  -cp 'lucille-all/target/lib/*' \
  com.kmwllc.lucille.core.Worker \
  <pipeline-name>
```

Start Workers before the Runner to avoid Kafka consumer group rebalancing delays while documents are already in flight.

### Start the Indexer

The Indexer consumes documents from the Kafka destination topic and sends batches to the configured search backend:

```bash
java \
  -Dlog4j2.configurationFile=classpath:lucille-log4j2.xml \
  -Dconfig.file=<PATH/TO/YOUR/CONFIG.conf> \
  -cp 'lucille-all/target/lib/*' \
  com.kmwllc.lucille.core.Indexer \
  <pipeline-name>
```

| Flag | What it does |
|---|---|
| `-Dlog4j2.configurationFile=...` | Selects the Lucille logging config. Without it, Log4j2 logs only errors |
| `-Dconfig.file=...` | Path to your HOCON configuration file |
| `-cp '...'` | lucille-core, lucille-logging, every plugin module, and their runtime dependencies |
| `Runner -distributed` | Starts the run and coordinates with Kafka |
| `Worker <pipeline-name>` | Processes documents through the named pipeline |
| `Indexer <pipeline-name>` | Writes processed documents to the configured backend |

With Docker, run each component from the `kmwtechnology/lucille-all` image: set `LUCILLE_ROLE` to `runner` (with `LUCILLE_OPTS=-distributed`), `worker` or `indexer`, and set `LUCILLE_PIPELINE` to the pipeline name for workers and indexers.

See the [troubleshooting guide]({{< relref "docs/operations/troubleshooting" >}}) if components don't connect as expected.

---

## Verifying Your Run

### During the Run

Lucille logs throughput and latency metrics periodically:

```
25/10/31 13:40:21 INFO WorkerPool: 27017 docs processed. One minute rate: 1787.10 docs/sec. Mean pipeline latency: 10.63 ms/doc.
25/10/31 13:40:22 INFO PublisherImpl: 37029 docs published. One minute rate: 3225.69 docs/sec. Waiting on 21014 docs.
25/10/31 13:40:22 INFO Indexer: 17016 docs indexed. One minute rate: 455.07 docs/sec. Mean backend latency: 6.90 ms/doc.
```

### At Completion

At the end of every run, Lucille prints a stage-by-stage performance summary followed by a final run result:

```
25/10/31 13:46:47 INFO Stage: Stage rename-title metrics. Docs processed: 200000. Mean latency: 0.0003 ms/doc. Children: 0. Errors: 0.
25/10/31 13:46:47 INFO Stage: Stage add-source-tag metrics. Docs processed: 200000. Mean latency: 0.3532 ms/doc. Children: 0. Errors: 0.
25/10/31 13:46:47 INFO Runner:
RUN SUMMARY: Success. 1/1 connectors complete. All published docs succeeded.
connector1: complete. 200000 docs succeeded. 0 docs failed. 0 docs dropped. Time: 416.47 secs.
25/10/31 13:46:47 INFO Runner: Run took 417.46 secs.
```

### Check Your Search Backend

After the run, verify that documents are visible in your target system. For Solr, a commit with `openSearcher=true` is required before documents appear in query results — Lucille does not issue a commit automatically. For Elasticsearch and OpenSearch, documents are available after the index refresh interval (default: 1 second).
