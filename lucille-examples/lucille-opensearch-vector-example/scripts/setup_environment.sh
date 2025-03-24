#!/bin/bash

# OpenSearch connection details
export OPENSEARCH_URL="http://34.23.62.171:9200"
# Security is disabled, no authentication needed
# export OPENSEARCH_USERNAME="admin"
# export OPENSEARCH_PASSWORD="StrongPassword123!"
export OPENSEARCH_INDEX="lucille_code_vectors2"

# Project path for the file connector
export PROJECT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# Print environment information
echo "Environment variables set:"
echo "OPENSEARCH_URL: $OPENSEARCH_URL"
echo "OPENSEARCH_INDEX: $OPENSEARCH_INDEX"
echo "PROJECT_PATH: $PROJECT_PATH"
