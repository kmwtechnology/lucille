#!/bin/bash
# Setup environment variables for Lucille OpenSearch Vector example

# Set the OpenSearch URL
export OPENSEARCH_URL=${OPENSEARCH_URL:-"http://localhost:9200/"}

# Set the OpenSearch index name
export OPENSEARCH_INDEX=${OPENSEARCH_INDEX:-"lucille_code_vectors"}

# Get the absolute path to the project root directory
export PROJECT_PATH=$(cd "$(dirname "$0")/.." && cd .. && pwd)

# Set Gemini API key if not already set
if [ -z "$GEMINI_API_KEY" ]; then
  echo "WARNING: GEMINI_API_KEY environment variable is not set."
  echo "Please set it with: export GEMINI_API_KEY='your-api-key-here'"
  echo "Or enter your Gemini API key now (input will be hidden):"
  read -s GEMINI_API_KEY
  export GEMINI_API_KEY
fi

echo "Environment variables set:"
echo "OPENSEARCH_URL: $OPENSEARCH_URL"
echo "OPENSEARCH_INDEX: $OPENSEARCH_INDEX"
echo "PROJECT_PATH: $PROJECT_PATH"
echo "GEMINI_API_KEY: ${GEMINI_API_KEY:0:5}... (truncated for security)"
echo ""
echo "You can now run the indexing process with: ./scripts/run_ingest.sh"
