# Lucille Project Indexer with Vectors to OpenSearch

This example demonstrates how to use Lucille to index project files with vector embeddings into OpenSearch, enabling powerful semantic search capabilities for code projects. The example processes the Lucille codebase itself, extracting text from various file types, chunking the content, generating vector embeddings, and indexing the results in OpenSearch.

## Features

- Indexes source code and documentation files from the Lucille project
- Extracts and preserves content type metadata using Apache Tika
- Splits text content into manageable chunks for better semantic retrieval
- Generates vector embeddings for each chunk using Google's Gemini API
- Stores content and vectors in OpenSearch for hybrid search (keyword + semantic)

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

Key configuration elements:

```hocon
# Extract text and metadata using Tika
{
  name: "TextExtractor"
  class: "com.kmwllc.lucille.tika.stage.TextExtractor"
  byte_array_field: "file_content"
  metadata_prefix: "tika_"
  tika_config_path: "conf/tika-config.xml"
}

# Map Tika metadata fields to standardized names
{
  name: "renameFields",
  class: "com.kmwllc.lucille.stage.RenameFields"
  fieldMapping: {
    "text": "content",
    "path": "full_path",
    "name": "filename",
    "tika__content_type": "content_type"
  }
  update_mode: "overwrite"
}

# Create separate documents for text chunks while preserving metadata
{
  class: "com.kmwllc.lucille.stage.EmitNestedChildren"
  drop_parent: true
  fields_to_copy: {
    "content_type": "content_type",
    "file_creation_date": "file_creation_date",
    "file_modification_date": "file_modification_date",
    "file_path": "file_path",
    "file_size_bytes": "file_size_bytes"
  }
  name: "emitChunks"
}
```

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
        }
      ],
      "should": [
        {
          "knn": {
            "chunk_vector": {
              "vector": [...], // Your query vector here
              "k": 10
            }
          }
        }
      ]
    }
  }
}
```

## Technical Details

- **Embedding Model**: This example uses Google's "text-embedding-004" model which produces 768-dimensional vectors.
- **API Integration**: The Google Gemini API is used to generate high-quality embeddings for text chunks.
- **Chunking Strategy**: Text is split into chunks of approximately 2000 characters with 10% overlap to provide focused, relevant search results.
- **Metadata Extraction**: Apache Tika is used to extract metadata, including content type, which is preserved during chunking.

## Control Commands

The following control commands are available:

```bash
# Check the status of the containers
./scripts/opensearch_control.sh status

# View logs from the containers
./scripts/opensearch_control.sh logs

# Stop the containers
./scripts/opensearch_control.sh stop

# Restart the containers
./scripts/opensearch_control.sh restart

# Remove all data and reset (WARNING: destroys all indexed data)
./scripts/opensearch_control.sh reset
```

## Customization

- Adjust the chunk size in the configuration file to optimize for your content
- Modify included/excluded file patterns based on your needs
- Adjust the embedding dimensions (if supported by the model)
- Customize the metadata fields extracted from Tika and preserved during chunking

## Troubleshooting

If your documents are missing expected metadata:

1. Check the Tika extraction stage in the configuration file
2. Verify the field mapping in the RenameFields stage
3. Ensure that fields are properly copied in the EmitNestedChildren stage
4. Check the OpenSearch mapping to ensure it supports all required fields
