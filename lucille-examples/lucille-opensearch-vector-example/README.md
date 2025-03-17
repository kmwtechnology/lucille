# Lucille Project Indexer with Vectors to OpenSearch

This example demonstrates how to use Lucille to index project files with vector embeddings into OpenSearch, enabling powerful semantic search capabilities for code projects. The example processes the Lucille codebase itself, extracting text from various file types, chunking the content, generating vector embeddings, and indexing the results in OpenSearch.

## Features

- Indexes source code and documentation files from the Lucille project
- Splits text content into manageable chunks for better semantic retrieval
- Generates vector embeddings for each chunk using Google's Gemini API
- Stores content and vectors in OpenSearch for hybrid search (keyword + semantic)

## Requirements

- OpenSearch instance running locally using Docker. See: [OpenSearch Docker Installation](https://opensearch.org/docs/latest/install/docker/)
- An OpenSearch index set up with the provided mapping that supports vector storage
- Java 11 or higher
- Maven for building and running the example
- Google Gemini API key

## Setting up OpenSearch

1. Start OpenSearch with Docker (if not already running):

   ```bash
   docker run -p 9200:9200 -p 9600:9600 -e "discovery.type=single-node" opensearchproject/opensearch:latest
   ```

2. Access the OpenSearch Dashboard dev tools at [http://localhost:5601/app/dev-tools#/console](http://localhost:5601/app/dev-tools#/console)

3. Create the index with vector support by copying the JSON from `gemini_index_mapping.json` into the dev tools console and executing:

   ```json
   PUT lucille_code_vectors
   {
     "settings": { ... },
     "mappings": { ... }
   }
   ```

   Alternatively, you can use the provided script:

   ```bash
   ./scripts/setup_environment.sh
   ```

## Configuration

Set up your environment variables using the provided script:

```bash
source ./scripts/setup_environment.sh
```

This sets the following default environment variables:

- `OPENSEARCH_URL`: URL to your OpenSearch instance (default: [http://localhost:9200/](http://localhost:9200/))
- `OPENSEARCH_INDEX`: The OpenSearch index to use (default: lucille_code_vectors)
- `PROJECT_PATH`: The absolute path to the Lucille project directory (automatically determined)
- `GEMINI_API_KEY`: Your Google Gemini API key (will prompt if not set)

You can also set these variables manually:

```bash
# The URL to your OpenSearch instance
export OPENSEARCH_URL=http://localhost:9200/

# The OpenSearch index to use (must be created with the provided mapping)
export OPENSEARCH_INDEX=lucille_code_vectors

# The absolute path to the Lucille project directory
export PROJECT_PATH=/path/to/lucille

# Your Google Gemini API key
export GEMINI_API_KEY=your-api-key-here
```

## Running the Example

1. Build the project:

   ```bash
   mvn clean package
   ```

2. Run the indexing process:

   ```bash
   ./scripts/run_ingest.sh
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

### Hybrid Search (Text + Vector)

```json
GET lucille_code_vectors/_search
{
  "query": {
    "bool": {
      "must": [
        {
          "match": {
            "file_extension": ".java"
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

## Customization

- Adjust the chunk size in the configuration file to optimize for your content
- Modify included/excluded file patterns based on your needs
- Adjust the embedding dimensions (if supported by the model)
