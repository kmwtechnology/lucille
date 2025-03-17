#!/bin/bash

# Exit on error
set -e

# Source the environment setup
source "$(dirname "$0")/setup_environment.sh"

# Get the directory where the script is located
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# Navigate to the parent directory
PROJECT_DIR=$(cd "$SCRIPT_DIR/.." && pwd)

echo "Creating OpenSearch index: $OPENSEARCH_INDEX"

# Remove trailing slash from OPENSEARCH_URL if present
OPENSEARCH_URL=$(echo "$OPENSEARCH_URL" | sed 's/\/$//')

# Check if OpenSearch is running
if ! curl -s "$OPENSEARCH_URL" > /dev/null; then
  echo "ERROR: OpenSearch is not running at $OPENSEARCH_URL"
  echo "Start OpenSearch with: ./scripts/opensearch_control.sh start"
  exit 1
fi

# First check if the index already exists
INDEX_EXISTS=$(curl -s -o /dev/null -w "%{http_code}" "$OPENSEARCH_URL/$OPENSEARCH_INDEX")
if [ "$INDEX_EXISTS" = "200" ]; then
  echo "WARNING: Index $OPENSEARCH_INDEX already exists."
  read -p "Do you want to delete and recreate it? (y/n): " -n 1 -r
  echo
  if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Deleting existing index..."
    curl -XDELETE "$OPENSEARCH_URL/$OPENSEARCH_INDEX"
  else
    echo "Keeping existing index. Exiting."
    exit 0
  fi
fi

# Determine which mapping file to use
MAPPING_FILE="$PROJECT_DIR/gemini_index_mapping.json"
if [ ! -f "$MAPPING_FILE" ]; then
  # Try alternative location
  MAPPING_FILE="$PROJECT_DIR/mapping/opensearch_vector_mappings.json"
  if [ ! -f "$MAPPING_FILE" ]; then
    echo "ERROR: Mapping file not found at either location."
    echo "  - $PROJECT_DIR/gemini_index_mapping.json"
    echo "  - $PROJECT_DIR/mapping/opensearch_vector_mappings.json"
    exit 1
  fi
fi

echo "Using mapping file: $MAPPING_FILE"

# Create the index with the mapping
echo "Creating index with vector mapping..."
curl -XPUT "$OPENSEARCH_URL/$OPENSEARCH_INDEX" \
  -H 'Content-Type: application/json' \
  -d @"$MAPPING_FILE"

echo ""
echo "Index $OPENSEARCH_INDEX created successfully."
echo "You can now run the indexing process with: ./scripts/run_ingest.sh"
