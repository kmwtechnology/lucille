#!/bin/bash

# OpenSearch connection details
export OPENSEARCH_URL="https://admin:StrongPassword123!@34.138.97.13:9200"
export OPENSEARCH_INDEX="ai-powered-search"

# Project path for the file connector
# export PROJECT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../" && pwd)"
export PROJECT_PATH="/Users/kevin/Downloads/AI-Powered_Search.pdf"

# Print environment information
echo "Environment variables set:"
echo "OPENSEARCH_URL: $OPENSEARCH_URL"
echo "OPENSEARCH_INDEX: $OPENSEARCH_INDEX"
echo "PROJECT_PATH: $PROJECT_PATH"
