# Lucille Project Indexer with Vectors to OpenSearch

This example demonstrates how to use Lucille to index project files with vector embeddings into OpenSearch, enabling powerful semantic search capabilities for code projects. The example processes the Lucille codebase itself, extracting text from various file types, chunking the content, generating vector embeddings, and indexing the results in OpenSearch.

## Features

- Indexes source code and documentation files from the Lucille project
- Extracts and preserves content type metadata using Apache Tika
- Splits text content into manageable chunks for better semantic retrieval
- Generates vector embeddings for each chunk using locally running Ollama
- Stores content and vectors in OpenSearch for hybrid search (keyword + semantic)
- Optimized configuration for Ollama API rate limits
- Local execution mode without Kafka/ZooKeeper dependencies

## Requirements

- Java 11 or higher
- Maven for building and running the example
- Access to the OpenSearch instance in Docker

## Complete Setup and Running Guide

### 1. Clone and Build the Project

If you haven't already, clone the Lucille repository and build the project:

```bash
git clone https://github.com/kmwtechnology/lucille.git
cd lucille
mvn clean install
```

### 2. Remote OpenSearch Environment

This example has been configured to use a remote OpenSearch instance deployed on GCP with the following details:

- OpenSearch URL: `http://localhost:9200`
- OpenSearch Dashboards URL: `http://localhost:5601`

### 3. Create the Vector-enabled Index

Create the index with the required mapping by running:

```bash
./scripts/create_index.sh
```

This script will:

- Set required environment variables
- Create an OpenSearch index with the correct vector mapping from `mapping/opensearch_vector_mappings.json`

### 4. Run the Ingest Process

Start the indexing process to extract text, generate embeddings, and index the data:

```bash
./scripts/run_ingest.sh
```

### 5. Verify the Indexing Results

You can check the indexed documents using OpenSearch Dashboards or with curl:

```bash
# Get total document count
curl -k -X GET "http://localhost:9200/lucille_code_vectors/_count?pretty"

# View a sample of documents 
curl -k -X POST "http://localhost:9200/lucille_code_vectors/_search?pretty" \
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
7. **OllamaEmbed**: Generates vector embeddings using Ollama

## Rate Limiting and Optimization

This example includes optimizations for respecting Ollama API rate limits:

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
  # Reduced threads to respect Ollama API rate limits
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

## Troubleshooting

If you encounter API rate limit errors, you can further adjust the throttling parameters in `conf/opensearch-vector.conf`:

- Reduce `worker.threads` to decrease concurrent requests
- Increase `sleepBetweenDocs` for longer pauses between documents
- Increase `throttleDelayMs` in the connector configuration
- Decrease `batchSize` values to process fewer documents at once

### Common SSL/HTTPS Issues

When working with the remote OpenSearch instance:

1. **Certificate Warnings**: These are expected with self-signed certificates
   - When using curl, always use the `-k` flag to bypass certificate validation
   - In browsers, you may need to click "Advanced" and "Proceed" to bypass warnings

2. **Connection Issues**:
   - Ensure proper credentials are being used in all requests
   - If getting authentication errors, make sure to include `-u admin:StrongPassword123!` with curl requests
   - For Java clients, make sure SSL verification is properly configured

## Additional Resources

- [Lucille Documentation](https://github.com/kmwtechnology/lucille)
- [OpenSearch Vector Search Documentation](https://opensearch.org/docs/latest/search-plugins/knn/index/)
- [Google Gemini API Documentation](https://ai.google.dev/docs/gemini_api_overview)
