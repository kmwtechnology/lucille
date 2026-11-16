---
title: Your First Pipeline
weight: 4
date: 2025-06-09
description: >
  A step-by-step walkthrough of writing a minimal Lucille config from scratch.
---

This page walks you through creating a Lucille configuration file from scratch — a single connector reading from a CSV file, a pipeline with two stages, and an indexer writing to a local CSV output. No search backend required.

By the end, you'll understand the structure of a Lucille config and be ready to adapt it for your own data.

## Prerequisites

- Lucille built from source (see [Installation]({{< relref "docs/getting-started/installation" >}}))
- A terminal in the Lucille repository root

## Step 1: Create a Source File

Create a file called `my-source.csv` with a small set of classic novels:

```csv
id,title,author,year
1,The Great Gatsby,F. Scott Fitzgerald,1925
2,To Kill a Mockingbird,Harper Lee,1960
3,1984,George Orwell,1949
4,Pride and Prejudice,Jane Austen,1813
5,The Catcher in the Rye,J.D. Salinger,1951
```

## Step 2: Write the Config File

This config reads `my-source.csv`, applies two field transformations to each row, and writes the results to a new CSV file. This config has three parts:

- **Connector** — a `FileConnector` with the CSV file handler reads `my-source.csv` and emits each row as a Document.
- **Pipeline** — two stages manipulate fields on each Document: one renames `title` to `book_title`, and one adds a static `source` tag to every Document.
- **Indexer** — a CSV indexer writes the transformed Documents to an output file. No search backend is needed for this first example.

Create a file called `my-first-pipeline.conf`:

```hocon
# ─── CONNECTORS ───────────────────────────────────────────────
# A list of connectors to run in sequence.
# Each connector reads from a source and publishes Documents.

connectors: [
  {
    # A name for this connector (appears in logs and run summary)
    name: "csv-reader"

    # The connector implementation class
    class: "com.kmwllc.lucille.connector.FileConnector"

    # Which pipeline should process this connector's documents
    pipeline: "my-pipeline"

    # Path(s) to the source file(s)
    paths: ["my-source.csv"]

    # Apply the CSV file handler to *.csv files.
    # Each CSV file is parsed row by row; one Document is emitted per row.
    # idField uses the "id" column as the Document id. Without it, ids are
    # generated from the file path and row number.
    fileHandlers: {
      csv: {
        idField: "id"
      }
    }
  }
]

# ─── PIPELINES ────────────────────────────────────────────────
# Each pipeline is a named sequence of Stages.
# Stages transform Documents in place before they reach the Indexer.

pipelines: [
  {
    name: "my-pipeline"

    stages: [
      # Stage 1: Rename "title" to "book_title"
      {
        name: "rename-title"
        class: "com.kmwllc.lucille.stage.RenameFields"
        fieldMapping: {
          "title": "book_title"
        }
      },

      # Stage 2: Add a static field to every document
      {
        name: "add-source-tag"
        class: "com.kmwllc.lucille.stage.SetStaticValues"
        staticValues: {
          source: "my-first-pipeline"
        }
      }
    ]
  }
]

# ─── INDEXER ──────────────────────────────────────────────────
# The Indexer sends processed Documents to a destination.
# We use the CSV indexer here so no search backend is needed.

indexer {
  # CSV indexer settings
  csv {
    # Which fields to include in the output (must match field names after pipeline processing)
    columns: ["id", "book_title", "author", "year", "source"]

    # Where to write the output
    path: "my-output.csv"

    # Include a header row
    includeHeader: true
  }
}
```

## Step 3: Run It

From the repository root:

```bash
java -Dlog4j2.configurationFile=classpath:lucille-log4j2.xml \
  -Dconfig.file=my-first-pipeline.conf \
  -cp 'lucille-core/target/lucille.jar:lucille-core/target/lib/*:lucille-logging/target/lucille-logging.jar:lucille-logging/target/lib/*' \
  com.kmwllc.lucille.core.Runner
```

The classpath needs `lucille-logging` as well as `lucille-core`: core ships only the SLF4J API, so without the logging module you get `No SLF4J providers were found` and no log output. The `-Dlog4j2.configurationFile` flag selects the Lucille logging config; without it, Log4j2 logs only errors.

Alternatively, run the published `kmwtechnology/lucille` Docker image, which already contains core and logging. Mount the directory holding your files and point `LUCILLE_CONF` at the config:

```bash
docker run --rm -v "$PWD":/lucille/work -w /lucille/work \
  -e LUCILLE_CONF=/lucille/work/my-first-pipeline.conf \
  kmwtechnology/lucille:1.0.0-alpha.0
```

The container runs as a non-root `lucille` user. On Linux, add `--user "$(id -u):$(id -g)"` if it can't write `my-output.csv` into the mounted directory.

## Step 4: Check the Output

Open `my-output.csv`:

```csv
"id","book_title","author","year","source"
"1","The Great Gatsby","F. Scott Fitzgerald","1925","my-first-pipeline"
"2","To Kill a Mockingbird","Harper Lee","1960","my-first-pipeline"
"3","1984","George Orwell","1949","my-first-pipeline"
"4","Pride and Prejudice","Jane Austen","1813","my-first-pipeline"
"5","The Catcher in the Rye","J.D. Salinger","1951","my-first-pipeline"
```

The `title` field was renamed to `book_title`, and a `source` field was added to every document.

## Step 5: Check the Logs

You should see output like:

```
... INFO ConfigValidator: Pipeline Configuration is valid.
... INFO ConfigValidator: Connector Configuration is valid.
... INFO ConfigValidator: Indexer Configuration is valid.
... INFO ConfigValidator: Other (Publisher, Runner, etc.) Configuration is valid.
... INFO Runner: Starting run with id <run-id>
... INFO WorkerPool: Starting 1 worker threads for pipeline my-pipeline
... INFO Runner: Running connector csv-reader feeding to pipeline my-pipeline
... INFO PublisherImpl: First doc published after ... ms
... INFO PublisherImpl: 5 docs published. 0 children created. 5 success events. 0 failure events. 0 drop events.
... INFO PublisherImpl: All documents SUCCEEDED.
... INFO Runner: Connector csv-reader feeding to pipeline my-pipeline complete. Time: ... secs.
... INFO Stage: Stage rename-title metrics. Docs processed: 5. Mean latency: ... ms/doc. Children: 0. Errors: 0.
... INFO Stage: Stage add-source-tag metrics. Docs processed: 5. Mean latency: ... ms/doc. Children: 0. Errors: 0.
... INFO Runner:

RUN SUMMARY: Success. 1/1 connectors complete. All published docs succeeded.

csv-reader: complete. 5 docs succeeded. 0 docs failed. 0 docs dropped. Time: ... secs.

... INFO Runner: Run took ... secs.
```

Each line starts with a timestamp and the run id, shown here as `...`. A few lines vary between runs: periodic `WorkerPool: N docs processed` metrics lines can appear, and the publisher's rate lines are omitted here.

## Understanding the Config Structure

Every Lucille config has three required parts:

| Block | Purpose |
|---|---|
| `connectors` | A list of data sources to read from, executed in sequence |
| `pipelines` | Named sequences of Stages that transform Documents |
| `indexer` | Where to send the processed Documents |

Each **connector** specifies:
- `name` — for logging and the run summary
- `class` — the fully qualified Java class that implements the connector
- `pipeline` — which pipeline processes this connector's output
- Connector-specific settings (e.g., `paths` for FileConnector)

Each **stage** in a pipeline specifies:
- `class` — the fully qualified Java class that implements the stage
- `name` (optional) — for logging and per-stage metrics
- Stage-specific parameters (e.g., `fieldMapping` for RenameFields)

The **indexer** specifies:
- Backend-specific settings in a nested block (e.g., `csv {}`, `solr {}`, `opensearch {}`), whose name selects the indexer
- Or `class` — for custom indexers

## What Have We Accomplished?

At first glance, the config above might feel disproportionate to the task. We renamed one field and added a static value — a transformation you could express in a single line of shell:

```bash
sed '1s/title/book_title/' my-source.csv \
  | awk -F, 'BEGIN{OFS=","} NR==1{print $0,"source"} NR>1{print $0,"my-first-pipeline"}' \
  > my-output.csv
```

That's fair. For this specific transformation on this specific file, a shell one-liner wins. The value of Lucille comes from what you can do next — and how little you have to change to get there.

### 1. Index to any search backend

Replace the CSV indexer with a real search backend — OpenSearch, Elasticsearch, or Solr — and your trivial file experiment becomes a real search ingestion pipeline. For OpenSearch:

```hocon
indexer {
  opensearch {
    url: "http://localhost:9200"
    index: "my-books"
  }
}
```

This needs the `lucille-opensearch` plugin on the classpath: run with the `kmwtechnology/lucille-all` image, or with `-cp 'lucille-all/target/lib/*'` after a source build. Other than that, the indexer block is the only change. The connector, the pipeline, the stages — everything else stays exactly the same. If you later want to switch from OpenSearch to Solr, you change the `indexer` block and add a `solr {}` block. No code, no build cycle, no rewrite.

### 2. Iterate on your pipeline

Add stages, chain transformations, and introduce conditions — all in config. Want to generate an embedding for each book title and store it as a vector field? Add a stage. Want to skip that stage for documents that already have an embedding? Add a condition. Browse the [Stage Reference]({{< relref "docs/ingest-design/stages" >}}) for the full library of available transformations.

None of this requires a build. Edit the config file, rerun Lucille, see the results. The pipeline is the config.

### 3. Scale

In our example, five rows processed in milliseconds and scale was not a concern. But now imagine the CSV has a million rows, each containing an S3 URL to a large PDF. Your pipeline must fetch each PDF, extract text with Tika, run OCR on scanned pages, and generate embeddings. That pipeline takes hours or days on a single thread.

**Step 1: add worker threads.** Lucille's local mode uses a thread pool for document processing. Increase the number of threads to parallelize across available CPU cores and overlap IO:

```hocon
worker {
  threads: 8
}
```

No other changes needed. The same config, more threads.

**Step 2: distribute across machines.** If one machine isn't enough, switch to distributed mode. Lucille's components — Runner, Workers, and Indexer — run as separate JVM processes and communicate through Kafka. Here's what changes:

- The Runner gets the `-distributed` flag: `com.kmwllc.lucille.core.Runner -distributed`
- Workers and the Indexer are started as separate processes on separate machines (or in separate Kubernetes pods): `com.kmwllc.lucille.core.Worker my-pipeline` and `com.kmwllc.lucille.core.Indexer my-pipeline`
- A `kafka {}` block with `bootstrapServers` is added to the config

Your connector definition and your pipeline stages are untouched. See [Production Deployment]({{< relref "docs/operations/deployment" >}}) for the full distributed mode setup.

---

At first, with a trivial five-row example that ran in under a second, Lucille looked like overkill compared to a shell script. But as soon as we wanted to ingest into a real search backend, iterate on enrichment logic, and scale out to handle large volumes of heavy documents, we could do all of that in a config-driven way — switching backends, adding stages, and distributing across machines — without having to solve any of those problems from scratch.

Consider what Lucille is doing on our behalf in the scaled-out version of this pipeline. The Runner is reading our million-row CSV and producing each row as a Document onto a Kafka topic, applying backpressure so it doesn't publish faster than Workers can consume. Multiple Worker processes — running on different machines or in different Kubernetes pods — are each pulling Documents off that topic in parallel, each running multiple threads, each thread independently fetching a PDF from S3, extracting text, running OCR, and generating embeddings. As each Document finishes processing, a lifecycle event is sent back to the Runner so it can track exactly how many Documents have succeeded, failed, or been dropped. If a Worker crashes mid-run, Kafka's consumer group protocol detects it and reassigns its unacknowledged Documents to another Worker — nothing is silently lost. If a single malformed PDF repeatedly crashes a Worker, Lucille routes it to a dead-letter topic after a configurable number of retries and keeps the rest of the run going. The Indexer is collecting processed Documents and sending them to the search backend in tunable batches, managing bulk API semantics, and handling retries on transient backend errors. Throughout the run, Lucille is logging throughput metrics, per-stage latency, and document counts at regular intervals. When the last Document reaches a terminal state, Lucille determines that the run is complete and prints a structured summary — how many succeeded, how many failed, how many were dropped, and how long it took.

All of that happens without any code on our part. Our contribution is the config file we wrote in Step 2.
