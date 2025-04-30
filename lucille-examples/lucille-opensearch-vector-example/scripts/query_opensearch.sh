#!/bin/bash

# Source the environment variables
source "$(dirname "$0")/setup_environment.sh"

# Check if a query was provided
if [ -z "$1" ]; then
  echo "Usage: $0 <query text>"
  echo "Example: $0 'vector search implementation'"
  exit 1
fi

QUERY_TEXT="$1"

echo "Searching for documents matching: '$QUERY_TEXT'"
echo

# Perform a simple text search
curl -k -X POST "${OPENSEARCH_URL}/${OPENSEARCH_INDEX}/_search" \
  -u "admin:StrongPassword123!" \
  -H 'Content-Type: application/json' \
  -d '{
    "size": 5,
    "query": {
      "match": {
        "chunk_text": "'"$QUERY_TEXT"'"
      }
    },
    "_source": ["chunk_text", "file_path", "content_type"]
  }'
