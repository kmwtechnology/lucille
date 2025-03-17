#!/bin/bash

# Script to check if vectors were properly indexed in OpenSearch

# Source environment setup
source "$(dirname "$0")/scripts/setup_environment.sh"

# Print script header
echo "===== OpenSearch Vector Check Tool ====="
echo "Checking vectors in: ${OPENSEARCH_INDEX}"
echo "OpenSearch URL: ${OPENSEARCH_URL}"
echo ""

echo "1. Checking document count in index..."
curl -s "${OPENSEARCH_URL}/${OPENSEARCH_INDEX}/_count" | jq .
echo ""

echo "2. Retrieving a sample document to verify structure..."
curl -s "${OPENSEARCH_URL}/${OPENSEARCH_INDEX}/_search?size=1" | jq '.hits.hits[0]._source | {id, parent_id, chunk_number, vector_dimensions: (.chunk_vector | length), file_extension, content_preview: (.chunk_text | tostring | .[0:100] + "...")}'
echo ""

echo "3. Checking how many documents have vectors..."
curl -s "${OPENSEARCH_URL}/${OPENSEARCH_INDEX}/_search" -H 'Content-Type: application/json' -d '
{
  "size": 0,
  "query": {
    "exists": {
      "field": "chunk_vector"
    }
  }
}' | jq '.hits.total'
echo ""

echo "Done! Your vectors are ready for semantic search."
