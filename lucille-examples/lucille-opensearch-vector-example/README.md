# Lucille Project Indexer with Vectors to OpenSearch

This example demonstrates how to use Lucille to index project files with vector embeddings into OpenSearch, enabling powerful semantic search capabilities for code projects. The example processes the Lucille codebase itself, extracting text from various file types, chunking the content, generating vector embeddings, and indexing the results in OpenSearch.

## Features

- Indexes source code and documentation files from the Lucille project
- Extracts and preserves content type metadata using Apache Tika
- Splits text content into manageable chunks for better semantic retrieval
- Generates vector embeddings for each chunk using Google's Gemini API
- Stores content and vectors in OpenSearch for hybrid search (keyword + semantic)
- Optimized configuration for Gemini API rate limits
- Local execution mode without Kafka/ZooKeeper dependencies

## Requirements

- Java 11 or higher
- Maven for building and running the example
- Google Gemini API key
- Docker and Docker Compose for running OpenSearch and OpenSearch Dashboards

## Complete Setup and Running Guide

### 1. Clone and Build the Project

If you haven't already, clone the Lucille repository and build the project:

```bash
git clone https://github.com/kmwtechnology/lucille.git
cd lucille
mvn clean install -DskipTests
```

### 2. Start OpenSearch Environment

Navigate to the example directory and start the OpenSearch environment:

```bash
cd lucille-examples/lucille-opensearch-vector-example
./scripts/opensearch_control.sh start
```

This will:

- Start an OpenSearch container (accessible at [http://localhost:9200](http://localhost:9200))
- Start an OpenSearch Dashboards container (accessible at [http://localhost:5601](http://localhost:5601))
- Configure both services with security disabled for ease of use in development

### 3. Create the Vector-enabled Index

Create the index with the required mapping by running:

```bash
./scripts/create_index.sh
```

This script will:

- Set required environment variables
- Prompt for your Google Gemini API key if not set
- Create an OpenSearch index with the correct vector mapping from `gemini_index_mapping.json`

### 4. Run the Ingest Process

Start the indexing process to extract text, generate embeddings, and index the data:

```bash
./scripts/run_ingest.sh
```

The ingest process performs the following steps:

1. Scans the project directory for source code files
2. Extracts text and metadata (including content type) using Apache Tika
3. Chunks the extracted text into smaller, semantically meaningful segments
4. Generates vector embeddings for each chunk using Google's Gemini API
5. Indexes both the text content and vector embeddings in OpenSearch

### 5. Verify the Indexing Results

You can check the indexed documents using OpenSearch Dashboards or with curl:

```bash
# Get total document count
curl -X GET "http://localhost:9200/lucille_code_vectors/_count?pretty"

# View a sample of documents 
curl -X POST "http://localhost:9200/lucille_code_vectors/_search?pretty" \
  -H 'Content-Type: application/json' \
  -d '{"query": {"match_all": {}}, "size": 5}'
```

## Understanding the Pipeline Configuration

The indexing pipeline is defined in `conf/opensearch-vector.conf` and includes the following key stages:

1. **File Connector**: Scans the project directory for files to process
2. **TextExtractor**: Uses Apache Tika to extract text and metadata from files
3. **RenameFields**: Maps Tika metadata fields to standardized field names
4. **CopyField**: Creates a copy of the text content for chunking
5. **ChunkText**: Splits text into smaller segments for better semantic retrieval
6. **EmitNestedChildren**: Creates separate documents for each text chunk
7. **GeminiEmbed**: Generates vector embeddings using Google's Gemini API

## Rate Limiting and Optimization

This example includes optimizations for respecting Gemini API rate limits:

```hocon
# File connector with throttling
{
  name: "fileConnector",
  # ...
  # Add throttling to control processing rate and respect API limits
  throttleDelayMs: 100
  batchSize: 5
}

# Worker configuration optimized for API rate limits
worker {
  pipeline: "pipeline1"
  # Reduced threads to respect Gemini API rate limits
  threads: 2
  # Add sleep between documents to control rate
  sleepBetweenDocs: 50
  # Add delay on processor errors
  sleepOnProcessorError: 500
}

# Publisher optimization
publisher {
  queueCapacity: 100
  batchSize: 10
  batchTimeout: 10000
}
```

These settings help prevent API rate limit errors by:

- Reducing the number of concurrent requests with fewer worker threads
- Adding delays between document processing
- Implementing backoff delays when errors occur
- Controlling batch sizes for optimal throughput

## Local Execution Mode

This example runs in local mode (`-local` flag) without requiring Kafka or ZooKeeper:

```bash
java -cp "$CLASSPATH" \
  -Dconfig.file="$CONFIG_FILE" \
  com.kmwllc.lucille.core.Runner \
  -local
```

This simplifies setup and is suitable for smaller indexing jobs. For larger production workloads, you may want to configure a distributed setup with Kafka.

## Searching the Data

Once indexed, you can perform various types of searches in OpenSearch. Here are some examples:

### Standard Text Search

```json
GET lucille_code_vectors/_search
{
  "query": {
    "match": {
      "chunk_text": "opensearch indexer"
    }
  }
}
```

### Search by Content Type

```json
GET lucille_code_vectors/_search
{
  "query": {
    "match": {
      "content_type": "application/java"
    }
  }
}
```

### Vector Similarity Search

```json
GET lucille_code_vectors/_search
{
  "query": {
    "knn": {
      "chunk_vector": {
        "vector": [...], // Your query vector here (768 dimensions)
        "k": 10
      }
    }
  }
}
```

### Hybrid Search (Text + Vector + Content Type)

```json
GET lucille_code_vectors/_search
{
  "query": {
    "bool": {
      "must": [
        {
          "match": {
            "content_type": "application/java"
          }
        },
        {
          "knn": {
            "chunk_vector": {
              "vector": [...], // Your query vector here
              "k": 10
            }
          }
        }
      ],
      "should": [
        {
          "match": {
            "chunk_text": "opensearch connector"
          }
        }
      ]
    }
  }
}
```

## Creating OpenSearch Backups

This example includes configuration for creating snapshots of your OpenSearch indices, which is useful for backups and data migration.

### Prerequisites

Ensure your `docker-compose.yml` has the following configuration (already included):

```yaml
opensearch:
  # ... other configurations ...
  environment:
    # ... other environment variables ...
    - "path.repo=/usr/share/opensearch/snapshots"
  volumes:
    # ... other volumes ...
    - ./opensearch_snapshots:/usr/share/opensearch/snapshots
```

### Step 1: Set up the Snapshot Directory

First, ensure your snapshot directory exists and has proper permissions:

```bash
# Check if the directory exists
docker exec opensearch ls -la /usr/share/opensearch/snapshots

# Set proper permissions (if needed)
docker exec opensearch chmod 777 /usr/share/opensearch/snapshots

# Verify permissions
docker exec opensearch ls -la /usr/share/opensearch/snapshots
```

### Step 2: Register the Snapshot Repository

Register a repository for storing your snapshots:

```bash
curl -X PUT "http://localhost:9200/_snapshot/my_backup" -H 'Content-Type: application/json' -d'
{
  "type": "fs",
  "settings": {
    "location": "/usr/share/opensearch/snapshots",
    "compress": true
  }
}'
```

This only needs to be done once unless you change repository settings.

### Step 3: Create a Snapshot

Create a snapshot of a specific index (e.g., `lucille_code_vectors`):

```bash
curl -X PUT "http://localhost:9200/_snapshot/my_backup/snapshot_name?wait_for_completion=true" -H 'Content-Type: application/json' -d'
{
  "indices": "lucille_code_vectors",
  "ignore_unavailable": true,
  "include_global_state": false
}'
```

Replace `snapshot_name` with a descriptive name (e.g., `snapshot_20250318`).

To back up all indices, omit the `indices` parameter:

```bash
curl -X PUT "http://localhost:9200/_snapshot/my_backup/snapshot_all?wait_for_completion=true" -H 'Content-Type: application/json' -d'
{
  "ignore_unavailable": true,
  "include_global_state": true
}'
```

### Step 4: Verify the Snapshot

Check that your snapshot completed successfully:

```bash
# List all snapshots in the repository
curl -X GET "http://localhost:9200/_snapshot/my_backup/_all"

# Check details of a specific snapshot
curl -X GET "http://localhost:9200/_snapshot/my_backup/snapshot_name"
```

### Step 5: Verify Local Files

Confirm that snapshot files are present on your local machine:

```bash
# List files in the snapshot directory
ls -la ./opensearch_snapshots
```

You should see files like `meta-*.dat`, `snap-*.dat`, and an `indices` directory.

## Troubleshooting

If you encounter API rate limit errors, you can further adjust the throttling parameters in `conf/opensearch-vector.conf`:

- Reduce `worker.threads` to decrease concurrent requests
- Increase `sleepBetweenDocs` for longer pauses between documents
- Increase `throttleDelayMs` in the connector configuration
- Decrease `batchSize` values to process fewer documents at once

## Additional Resources

- [Lucille Documentation](https://github.com/kmwtechnology/lucille)
- [OpenSearch Vector Search Documentation](https://opensearch.org/docs/latest/search-plugins/knn/index/)
- [Google Gemini API Documentation](https://ai.google.dev/docs/gemini_api_overview)
