# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Quick Start Commands

```bash
# Build the entire project
mvn clean install

# Build a specific module (e.g., lucille-core)
mvn clean install -pl lucille-core

# Run tests for the entire project
mvn test

# Run tests for a specific module
mvn test -pl lucille-core

# Run a specific test class
mvn test -pl lucille-core -Dtest=WorkerPoolTest

# Run a specific test method
mvn test -pl lucille-core -Dtest=WorkerPoolTest#testSpecificMethod

# Run integration tests (Failsafe) for a specific module
mvn failsafe:integration-test -pl lucille-core
```

## Project Overview

**Lucille** is a production-grade Search ETL (Extract-Transform-Load) framework designed to help you get data into search engines like Apache Solr, Elasticsearch, OpenSearch, Pinecone, and Weaviate.

### Core Architecture

The system operates as a **pipeline with 4 main thread types**:

1. **Connector Thread**: Reads data from a source system and publishes documents to a pipeline
2. **Worker Thread(s)**: Processes documents through a series of transformation stages
3. **Indexer Thread**: Takes processed documents and indexes them into a target search engine
4. **Main Thread**: Orchestrates the entire run and monitors completion

### Key Components

#### Runner (`lucille-core/src/main/java/com/kmwllc/lucille/core/Runner.java`)
- Entry point for executing a Lucille "run"
- Coordinates the execution of one or more Connectors sequentially
- Supports both local in-memory execution and distributed Kafka-based execution
- Uses CLI arguments to specify configuration file path

#### Connector (`lucille-core/src/main/java/com/kmwllc/lucille/core/Connector.java`)
- Interface for reading data from external sources
- Must implement `preExecute()`, `execute()`, and `postExecute()` lifecycle methods
- Publishes documents to a named pipeline via a Publisher
- Implementations must have a constructor accepting a single `Config` parameter for reflective instantiation

#### Stage (`lucille-core/src/main/java/com/kmwllc/lucille/core/Stage.java`)
- Performs transformations on documents as they flow through a pipeline
- Stages are applied sequentially to each document
- Supports conditional execution based on field values
- All implementations must declare a `public static Spec SPEC` for configuration validation

#### Indexer (`lucille-core/src/main/java/com/kmwllc/lucille/core/Indexer.java`)
- Interface for writing processed documents to a target search engine
- Separate configuration for generic indexer settings and implementation-specific settings (e.g., "solr", "opensearch")

#### Document (`lucille-core/src/main/java/com/kmwllc/lucille/core/Document.java`)
- Represents a searchable item flowing through the pipeline
- Can be created as `JsonDocument` or `HashMapDocument`
- Contains fields that flow through stages and eventually get indexed

### Configuration (HOCON Format)

Configuration uses Typesafe Config library with HOCON format. Three main sections:

1. **connectors**: List of data source connectors to execute sequentially
2. **pipelines**: Named pipelines with ordered lists of stages to process documents
3. **indexer**: Configuration for destination search engine

Example configuration structure:
```hocon
connectors: [
  {
    name: "my-connector"
    class: "com.kmwllc.lucille.connector.FileConnector"
    pipeline: "my-pipeline"
    # Connector-specific config
  }
]

pipelines: [
  {
    name: "my-pipeline"
    stages: [
      {
        name: "stage1"
        class: "com.kmwllc.lucille.stage.SomeStage"
        # Stage-specific config
      }
    ]
  }
]

indexer: {
  class: "com.kmwllc.lucille.indexer.SolrIndexer"
  # Indexer-specific config
}
```

### Project Structure

- **lucille-parent**: Parent POM with dependency management and shared plugin configuration
- **lucille-bom**: Bill of Materials for managing transitive dependencies
- **lucille-core**: Core runtime engine (Connector, Stage, Indexer interfaces and Worker/Indexer threads)
- **lucille-plugins**: Community connectors, stages, and indexers
  - `lucille-api`: REST API stage
  - `lucille-entity-extraction`: Named entity recognition stages
  - `lucille-jlama`: LLM integration via JLama
  - `lucille-ocr`: Optical character recognition
  - `lucille-tika`: Apache Tika document parsing
  - And others for Parquet, Pinecone, Weaviate integrations
- **lucille-examples**: Runnable examples demonstrating common use cases

### Important Patterns

#### Creating a Custom Connector
1. Implement the `Connector` interface
2. Provide a constructor accepting a `Config` parameter
3. Use the Publisher passed to `execute()` to publish documents
4. Respect the lifecycle: `preExecute()` → `execute()` → `postExecute()` → `close()`

#### Creating a Custom Stage
1. Extend the `Stage` abstract class
2. Declare a `public static Spec SPEC` defining configuration parameters
3. Override `processDocument(Document doc)` to transform the document
4. Call `super(config)` which validates config against the Spec
5. Support conditional execution via inherited condition fields (fields, values, operator)

#### Specs and Configuration
- Stages use `Spec` objects to define and validate their configuration properties
- `SpecBuilder` provides a fluent API for building Specs
- Properties can be required or optional with default values
- Validation errors include the stage name for easier debugging

## Concurrency Model

- **Local mode**: All operations (Connector, Workers, Indexer, Publisher polling) run as threads in a single JVM
- **Distributed mode**: Multiple Worker and Indexer processes poll from Kafka for work, while Runner publishes work to Kafka
- **WorkerPool**: Manages a thread pool for parallel document processing (default 4 threads per lucille-core config)
- **WorkerIndexerPool**: Similar pool for indexing operations

## Running Examples

### OpenSearch Vector Ollama Example
Located in: `lucille-examples/lucille-opensearch-vector-ollama-example/`

This example demonstrates **hybrid search** combining keyword-based BM25 scoring with semantic vector embeddings:

**Prerequisites:**
- Docker and Docker Compose
- Ollama running locally with `nomic-embed-text:latest` model
- Maven 3.8+

**Quick Start:**
```bash
cd lucille-examples/lucille-opensearch-vector-ollama-example

# Start OpenSearch and Dashboards
docker compose up -d

# Build the project
mvn clean install -DskipTests

# Prepare test data directory
mkdir -p /tmp/lucille_test_data
cp sample-documents/* /tmp/lucille_test_data/

# Run the pipeline
export OPENSEARCH_URL="http://localhost:9200"
export OPENSEARCH_INDEX="tech-docs"
export OLLAMA_URL="http://localhost:11434"
export OLLAMA_MODEL="nomic-embed-text:latest"
export PROJECT_PATH="/tmp/lucille_test_data"

java -Dconfig.file=conf/opensearch-vector.conf \
  -cp 'target/lib/*' \
  -DOPENSEARCH_URL="${OPENSEARCH_URL}" \
  -DOPENSEARCH_INDEX="${OPENSEARCH_INDEX}" \
  -DOLLAMA_URL="${OLLAMA_URL}" \
  -DOLLAMA_MODEL="${OLLAMA_MODEL}" \
  -DPROJECT_PATH="${PROJECT_PATH}" \
  com.kmwllc.lucille.core.Runner

# View results in OpenSearch Dashboards
# http://localhost:5601
```

**Pipeline Stages:**
1. **FileConnector**: Reads documents from filesystem (supports 30+ formats via Tika)
2. **TextExtractor** (optional): Extracts text from binary files
3. **ApplyOpenNLPNameFinders** (optional): Named entity recognition
4. **EmbeddingsOllama**: Generates 768-dimensional vector embeddings
5. **OpenSearchIndexer**: Indexes documents with vectors for k-NN search

**Key Configuration Files:**
- `conf/opensearch-vector.conf`: Main pipeline configuration
- `mapping/opensearch_vector_mappings.json`: Index mappings with HNSW vector configuration
- `docker-compose.yml`: Container setup for OpenSearch 3.3.0 and Dashboards

**Hybrid Search Capabilities:**
- **Keyword Search**: Traditional BM25 scoring on document content
- **Semantic Search**: k-NN queries on 768-dimensional embeddings with cosine similarity
- **Combined**: Use both signals for optimal relevance

## Dependencies and Versions

- **Java**: 17+
- **Maven**: Recent version required
- **Key libraries**:
  - Typesafe Config 1.4.1 (configuration)
  - Jackson 2.17.0 (JSON serialization)
  - Lucene 9.11.1 (search core dependency)
  - Solr 9.8.0 (Apache Solr integration)
  - OpenSearch Java 2.11.1 (OpenSearch integration)
  - OpenSearch 3.3.0 (in examples with Docker)
  - Kafka 4.0.0 (distributed messaging)
  - Log4j 2.20.0 (logging)
  - Apache Tika 2.9+ (document parsing)
  - Ollama (external service for embeddings)
  - OpenNLP 2.3.0+ (NLP stages)
