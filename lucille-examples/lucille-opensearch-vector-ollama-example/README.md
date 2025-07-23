# Lucille Project Indexer with Vectors to OpenSearch

This example demonstrates how to use Lucille to index project files with vector embeddings into OpenSearch, enabling powerful semantic search capabilities for code projects. The example processes files designated in the PROJECT_PATH environment variable, extracting text from various file types, chunking the content, generating vector embeddings using Ollama, and indexing the results in OpenSearch.

## Features

- Indexes files from the PROJECT_PATH environment variable which can be a single file or a directory
- Extracts and preserves content type metadata using Apache Tika
- Splits text content into manageable chunks for better semantic retrieval such as a RAG application might need
- Generates vector embeddings for each chunk using locally running Ollama
- Stores content/context and vectors in OpenSearch for hybrid search (keyword + semantic)
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
mvn clean install -DskipTests
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

- Use OPENSEARCH_URL and OPENSEARCH_INDEX from your environment variables
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

1. **File Connector**: Scans the PROJECT_PATH directory for files to process
2. **TextExtractor**: Uses Apache Tika to extract text and metadata from files
3. **RenameFields**: Maps Tika metadata fields to standardized field names
4. **CopyField**: Creates a copy of the text content for chunking
5. **ChunkText**: Splits text into smaller segments for better semantic retrieval
6. **EmitNestedChildren**: Creates separate documents for each text chunk
7. **OllamaEmbed**: Generates vector embeddings using Ollama and store them with the document

## Local Execution 

run this script from within ./lucille-opensearch-vector-ollama-example directory via ./scripts/run_ingest.sh

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

## Additional Resources

- [Lucille Documentation](https://github.com/kmwtechnology/lucille)
- [OpenSearch Vector Search Documentation](https://opensearch.org/docs/latest/search-plugins/knn/index/)
- [Ollama API Documentation](https://ollama.readthedocs.io/en/api/)
- [Ollama JDK](https://github.com/ollama4j/ollama4j)