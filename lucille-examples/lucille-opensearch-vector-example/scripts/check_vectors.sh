#!/bin/bash

# Source the environment variables
source "$(dirname "$0")/setup_environment.sh"

echo "Checking the status of the index: ${OPENSEARCH_INDEX}"
curl -k -X GET "${OPENSEARCH_URL}/${OPENSEARCH_INDEX}/_stats" \
  -u "admin:StrongPassword123!" \
  -H 'Content-Type: application/json'

echo -e "\n\nChecking document count:"
curl -k -X GET "${OPENSEARCH_URL}/${OPENSEARCH_INDEX}/_count" \
  -u "admin:StrongPassword123!" \
  -H 'Content-Type: application/json'

echo -e "\n\nChecking random document with vector information:"
curl -k -X POST "${OPENSEARCH_URL}/${OPENSEARCH_INDEX}/_search" \
  -u "admin:StrongPassword123!" \
  -H 'Content-Type: application/json' \
  -d '{
    "size": 1,
    "query": {
      "exists": {
        "field": "chunk_vector"
      }
    }
  }'
