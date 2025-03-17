#!/bin/bash
# Setup environment variables for Lucille OpenSearch Vector example

# Set the OpenSearch URL
export OPENSEARCH_URL=${OPENSEARCH_URL:-"http://localhost:9200/"}

# Set the OpenSearch index name
export OPENSEARCH_INDEX=${OPENSEARCH_INDEX:-"lucille_code_vectors"}

# Get the absolute path to the project root directory
export PROJECT_PATH=$(cd "$(dirname "$0")/.." && cd .. && cd .. && pwd)

# Set Gemini API key directly
export GEMINI_API_KEY=AIzaSyCzvZfgulZOjw8CXDjZejvJwaqjAp3rDpA

echo "Environment variables set:"
echo "OPENSEARCH_URL: $OPENSEARCH_URL"
echo "OPENSEARCH_INDEX: $OPENSEARCH_INDEX"
echo "PROJECT_PATH: $PROJECT_PATH"
echo "GEMINI_API_KEY: ${GEMINI_API_KEY:0:5}... (truncated for security)"
echo ""
echo "You can now run the indexing process with: ./scripts/run_ingest.sh"
