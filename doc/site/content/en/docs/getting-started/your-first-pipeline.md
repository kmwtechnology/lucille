---
title: Your First Pipeline
weight: 3
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

Create a file called `my-source.csv` with some sample data:

```csv
id,title,author,year
1,The Great Gatsby,F. Scott Fitzgerald,1925
2,To Kill a Mockingbird,Harper Lee,1960
3,1984,George Orwell,1949
4,Pride and Prejudice,Jane Austen,1813
5,The Catcher in the Rye,J.D. Salinger,1951
```

## Step 2: Write the Config File

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

    # Tell the FileConnector how to handle CSV files
    fileHandlers: {
      csv: {}
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
        class: "com.kmwllc.lucille.stage.SetStaticFieldValue"
        fieldName: "source"
        fieldValue: "my-first-pipeline"
      }
    ]
  }
]

# ─── INDEXER ──────────────────────────────────────────────────
# The Indexer sends processed Documents to a destination.
# We use the CSV indexer here so no search backend is needed.

indexer {
  type: "CSV"
}

# CSV indexer settings
csv {
  # Which fields to include in the output (must match field names after pipeline processing)
  columns: ["id", "book_title", "author", "year", "source"]

  # Where to write the output
  path: "my-output.csv"

  # Include a header row
  includeHeader: true
}
```

## Step 3: Run It

```bash
java -Dconfig.file=my-first-pipeline.conf \
  -cp 'lucille-core/target/lucille-core-*.jar:lucille-core/target/lib/*' \
  com.kmwllc.lucille.core.Runner
```

## Step 4: Check the Output

Open `my-output.csv`:

```csv
id,book_title,author,year,source
1,The Great Gatsby,F. Scott Fitzgerald,1925,my-first-pipeline
2,To Kill a Mockingbird,Harper Lee,1960,my-first-pipeline
3,1984,George Orwell,1949,my-first-pipeline
4,Pride and Prejudice,Jane Austen,1813,my-first-pipeline
5,The Catcher in the Rye,J.D. Salinger,1951,my-first-pipeline
```

The `title` field was renamed to `book_title`, and a `source` field was added to every document.

## Step 5: Check the Logs

You should see output like:

```
INFO  Runner: Pipeline Configuration is valid.
INFO  Runner: Connector Configuration is valid.
INFO  Runner: Indexer Configuration is valid.
INFO  Runner: Starting run with id ...
INFO  Runner: Running connector csv-reader feeding to pipeline my-pipeline
INFO  PublisherImpl: First doc published after ... ms
INFO  WorkerPool: 5 docs processed. One minute rate: ... docs/sec. Mean pipeline latency: ... ms/doc.
INFO  Stage: Stage rename-title metrics. Docs processed: 5. Mean latency: ... ms/doc. Children: 0. Errors: 0.
INFO  Stage: Stage add-source-tag metrics. Docs processed: 5. Mean latency: ... ms/doc. Children: 0. Errors: 0.
INFO  Runner: RUN SUMMARY: Success. 1/1 connectors complete. All published docs succeeded.
INFO  Runner: Run took ... secs.
```

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
- `type` — a shorthand for built-in indexers (Solr, OpenSearch, Elasticsearch, CSV)
- Or `class` — for plugin or custom indexers
- Backend-specific settings in a separate block (e.g., `csv {}`, `solr {}`, `opensearch {}`)

## Next Steps

**Index to a search backend.** Replace the CSV indexer with a real search backend:

```hocon
indexer {
  type: "OpenSearch"
}

opensearch {
  url: "http://localhost:9200"
  url: ${?OPENSEARCH_URL}
  index: "my-books"
}
```

**Add more stages.** Browse the [Stage Reference]({{< relref "docs/reference/stages/stages_reference" >}}) for available transformations — text extraction, NER, embeddings, field manipulation, and more.

**Add conditions.** Skip stages for documents that don't need them:

```hocon
{
  class: "com.kmwllc.lucille.stage.OpenAIEmbed"
  source: "body"
  conditions: [
    { fields: ["body"], operator: "must" }
  ]
}
```

**Use environment variables.** Keep credentials out of your config file:

```hocon
opensearch {
  url: "http://localhost:9200"
  url: ${?OPENSEARCH_URL}
}
```

**Run in test mode.** Validate your pipeline without a search backend:

```bash
java -Dconfig.file=my-first-pipeline.conf \
  -cp 'lucille-core/target/lucille-core-*.jar:lucille-core/target/lib/*' \
  com.kmwllc.lucille.core.Runner -validate
```

**Scale out.** When your data grows, switch to distributed mode with no pipeline code changes. See [Production Deployment]({{< relref "docs/operations/deployment" >}}).

For the full configuration schema and all available options, see [Configuration Management]({{< relref "docs/operations/configuration" >}}).
