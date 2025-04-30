#!/bin/bash

# Source the environment setup
source "$(dirname "$0")/setup_environment.sh"

echo "Running search examples against OpenSearch index: $OPENSEARCH_INDEX"
echo "-------------------------------------------------------------------------"

# Text search example
echo "Example 1: Basic text search for 'opensearch connector'"
curl -XGET "$OPENSEARCH_URL$OPENSEARCH_INDEX/_search" \
  -H 'Content-Type: application/json' \
  -d '{
  "query": {
    "match": {
      "chunk_text": "opensearch connector"
    }
  },
  "_source": ["filename", "file_extension", "chunk_id", "chunk_text"],
  "size": 3
}'

echo -e "\n\n-------------------------------------------------------------------------"
# File type filter search
echo "Example 2: Find Java files that mention 'vector'"
curl -XGET "$OPENSEARCH_URL$OPENSEARCH_INDEX/_search" \
  -H 'Content-Type: application/json' \
  -d '{
  "query": {
    "bool": {
      "must": [
        {
          "term": {
            "file_extension": ".java"
          }
        },
        {
          "match": {
            "chunk_text": "vector"
          }
        }
      ]
    }
  },
  "_source": ["filename", "file_extension", "chunk_id", "chunk_text"],
  "size": 3
}'

echo -e "\n\n-------------------------------------------------------------------------"
# Get document count
echo "Example 3: Count of indexed documents by file extension"
curl -XGET "$OPENSEARCH_URL$OPENSEARCH_INDEX/_search" \
  -H 'Content-Type: application/json' \
  -d '{
  "size": 0,
  "aggs": {
    "file_types": {
      "terms": {
        "field": "file_extension",
        "size": 20
      }
    }
  }
}'

echo -e "\n\n-------------------------------------------------------------------------"
echo "Note: To perform vector similarity search, you need to generate an embedding vector"
echo "for your query. You can use the JLama library for this or an external API."
echo "Example query structure for vector search:"
echo '{
  "query": {
    "knn": {
      "chunk_vector": {
        "vector": [...], // 384-dimensional vector from the same model
        "k": 10
      }
    }
  }
}'
