# Lucille Project Indexer with Vectors (Ollama) to OpenSearch to enable hybrid search

This example demonstrates how to use Lucille to index project files with vector embeddings into OpenSearch, enabling powerful semantic and lexical search capabilities for code projects. The example processes files designated in the PROJECT_PATH environment variable, extracting text from various file types, performing entity extraction, chunking the content, enriching chunks with entity context, generating vector embeddings using Ollama, and indexing the results in OpenSearch.

## Features

- **File Processing**: Indexes files from the PROJECT_PATH environment variable (single file or directory)
- **Content Extraction**: Extracts and preserves content type metadata using Apache Tika
- **Entity Extraction**: Uses OpenNLP to extract people, organizations, and locations with confidence threshold tuning (0.85)
- **Entity Enrichment**: Appends extracted entity information to text chunks for improved semantic context
- **Text Chunking**: Splits text content into manageable chunks (1500 characters) for better semantic retrieval
- **Vector Embeddings**: Generates vector embeddings for each enriched chunk using locally running Ollama
- **Hybrid Search**: Stores content, entities, and vectors in OpenSearch for multi-modal search (keyword + semantic + entity-based)
- **Local Execution**: Runs without Kafka/ZooKeeper dependencies for simplified development

## Requirements

- Java 11 or higher
- Maven for building and running the example
- Docker and Docker Compose for running OpenSearch locally
- Ollama Desktop (for generating vector embeddings and performing LLM calls)
- OpenNLP models for entity extraction (automatically downloaded on first run)

### Environment Variables

- `PROJECT_PATH`: Path to the project files to be indexed (can be a single file or a directory)
- `OPENSEARCH_URL`: URL of the OpenSearch instance (default: `http://localhost:9200`)
- `OPENSEARCH_INDEX`: Name of the OpenSearch index to create (default: `tech-docs`)
- `OLLAMA_URL`: URL of the Ollama instance (default: `http://localhost:11434`)
- `OLLAMA_MODEL`: Ollama model to use for generating embeddings (default: `nomic-embed-text:latest` with 768 dimensions for embeddings using this model)

### Entity Extraction Configuration

- **Confidence Threshold**: 0.85 (filters low-confidence entity extractions)
- **Entity Types**: People, Organizations, Locations
- **Field Names**: `person`, `organization`, `location` (lowercase for consistency)
- **Models**: OpenNLP pre-trained models (en-ner-person.bin, en-ner-organization.bin, en-ner-location.bin)

## Complete Setup and Running Guide

### 1. Clone and Build the Project

If you haven't already, clone the Lucille repository and build the project:

```bash
git clone https://github.com/kmwtechnology/lucille.git
cd lucille
mvn clean install
```

### 2. Set Up OpenSearch with Docker

This example uses Docker Compose to run OpenSearch locally. The configuration includes OpenSearch and OpenSearch Dashboards with security disabled for development purposes.

#### Start OpenSearch Services

Navigate to the example directory and start the services:

```bash
cd lucille-examples/lucille-opensearch-vector-ollama-example
docker-compose up -d
```

This will start:

- **OpenSearch**: Available at `http://localhost:9200`
- **OpenSearch Dashboards**: Available at `http://localhost:5601`

#### Verify OpenSearch is Running

Wait for the services to start (this may take a few minutes), then verify OpenSearch is accessible:

```bash
curl -X GET "http://localhost:9200/_cluster/health?pretty"
```

You should see a response indicating the cluster status is "green" or "yellow".

#### Access OpenSearch Dashboards

Open your browser and navigate to `http://localhost:5601` to access the OpenSearch Dashboards interface.

#### Stop Services (when done)

To stop the OpenSearch services:

```bash
docker-compose down
```

To stop and remove all data:

```bash
docker-compose down -v
```

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
curl -k -X GET "http://localhost:9200/{YOUR_INDEX_NAME}/_count?pretty"

# View a sample of documents 
curl -k -X POST "http://localhost:9200/{YOUR_INDEX_NAME}/_search?pretty" \
  -H 'Content-Type: application/json' \
  -d '{"query": {"match_all": {}}, "size": 5}'
```

## Understanding the Pipeline Configuration

The indexing pipeline is defined in `conf/opensearch-vector.conf` and includes the following key stages:

### Document Processing Pipeline

1. **File Connector**: Scans the PROJECT_PATH directory for files to process
2. **TextExtractor**: Uses Apache Tika to extract text and metadata from files
3. **File Metadata Processing**: Extracts filename from Tika metadata and parses file path for extension
4. **RenameFields**: Maps Tika metadata fields to standardized field names (`text` → `content`, `path` → `full_path`)
5. **Content Type Processing**: Sets default content type and copies Tika content type information
6. **ApplyOpenNLPNameFinders**: Extracts entities (people, organizations, locations) using OpenNLP with 0.85 confidence threshold
7. **CopyFields**: Creates copies of content field to `chunk_text` for processing
8. **ChunkText**: Splits text into 1500-character segments with 25% overlap for better semantic retrieval
9. **EmitNestedChildren**: Creates separate documents for each text chunk, copying metadata fields

### Entity Enrichment Pipeline

1. **enrichChunkTextWithEntities**: Appends entity information to each chunk using format:

   ```text
   {chunk_content} | Entities: People: {person} Organizations: {organization} Locations: {location}
   ```

2. **copyEnrichedText**: Copies the enriched text back to the chunk_text field
3. **EmbeddingsOllama**: Generates vector embeddings for the entity-enriched chunks using Ollama

### Key Pipeline Optimizations

- **Entity Extraction Before Chunking**: Entities are extracted from the full document before text chunking to ensure complete context
- **Entity Enrichment After Chunking**: Each individual chunk is enriched with entity context to prevent truncation
- **Confidence Threshold Tuning**: 0.85 threshold filters low-quality entity extractions for better precision
- **Consistent Entity Fields**: Uses lowercase field names (`person`, `organization`, `location`) for better query consistency

## Local Execution

run this script from within ./lucille-opensearch-vector-ollama-example directory via ./scripts/run_ingest.sh

## Searching the Data

Once indexed, you can perform various types of searches in OpenSearch. The entity-enriched chunks enable powerful multi-modal search capabilities. Here are examples you can use in OpenSearch Dashboards (Dev Tools):

### Standard Text Search

```json
GET {YOUR_INDEX_NAME}/_search
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
GET {YOUR_INDEX_NAME}/_search
{
  "query": {
    "match": {
      "content_type": "application/java"
    }
  }
}
```

### Entity-Based Search

#### Search by People

```json
GET {YOUR_INDEX_NAME}/_search
{
  "query": {
    "match": {
      "person": "John Smith"
    }
  }
}
```

#### Search by Organizations

```json
GET {YOUR_INDEX_NAME}/_search
{
  "query": {
    "match": {
      "organization": "OpenSearch"
    }
  }
}
```

#### Search by Locations

```json
GET {YOUR_INDEX_NAME}/_search
{
  "query": {
    "match": {
      "location": "California"
    }
  }
}
```

### Multi-Entity Search

```json
GET {YOUR_INDEX_NAME}/_search
{
  "query": {
    "bool": {
      "must": [
        {
          "match": {
            "person": "Brian"
          }
        },
        {
          "match": {
            "organization": "UI"
          }
        }
      ]
    }
  }
}
```

### Vector Similarity Search

```json
GET {YOUR_INDEX_NAME}/_search
{
  "query": {
    "knn": {
      "embedding": {
        "vector": [...], // Your query vector here (768 dimensions)
        "k": 10
      }
    }
  }
}
```

### Hybrid Search (Text + Vector + Content Type)

```json
GET {YOUR_INDEX_NAME}/_search
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
            "embedding": {
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

### Entity-Enhanced Hybrid Search

```json
GET {YOUR_INDEX_NAME}/_search
{
  "query": {
    "bool": {
      "must": [
        {
          "knn": {
            "embedding": {
              "vector": [...], // Your query vector here
              "k": 20
            }
          }
        }
      ],
      "filter": [
        {
          "match": {
            "organization": "OpenSearch"
          }
        },
        {
          "match": {
            "content_type": "text/java"
          }
        }
      ],
      "should": [
        {
          "match": {
            "person": "developer"
          }
        },
        {
          "match": {
            "location": "California"
          }
        }
      ]
    }
  }
}
```

## Additional Resources

### Core Technologies

- [Lucille Documentation](https://github.com/kmwtechnology/lucille)
- [OpenSearch Vector Search Documentation](https://opensearch.org/docs/latest/search-plugins/knn/index/)
- [Ollama API Documentation](https://ollama.readthedocs.io/en/api/)
- [Ollama JDK](https://github.com/ollama4j/ollama4j)

### Entity Extraction & NLP

- [Apache OpenNLP Documentation](https://opennlp.apache.org/docs/)
- [OpenNLP Pre-trained Models](https://opennlp.apache.org/models.html)
- [Named Entity Recognition Guide](https://opennlp.apache.org/docs/2.3.0/manual/opennlp.html#tools.namefind)

### Vector Embeddings & Search

- [Nomic Embed Text Model](https://ollama.com/library/nomic-embed-text)
- [OpenSearch KNN Plugin](https://opensearch.org/docs/latest/search-plugins/knn/)
- [FAISS Vector Search Engine](https://github.com/facebookresearch/faiss)
- [Cosine Similarity in Vector Search](https://opensearch.org/docs/latest/search-plugins/knn/knn-index/#spaces)

### Content Processing

- [Apache Tika Documentation](https://tika.apache.org/)
- [Supported File Formats](https://tika.apache.org/2.9.1/formats.html)
- [HOCON Configuration Format](https://github.com/lightbend/config/blob/main/HOCON.md)

### Development & Deployment

- [Docker Compose for OpenSearch](https://opensearch.org/docs/latest/install-and-configure/install-opensearch/docker/)
- [OpenSearch Dashboards](https://opensearch.org/docs/latest/dashboards/)
- [Maven Build Tool](https://maven.apache.org/guides/getting-started/)
