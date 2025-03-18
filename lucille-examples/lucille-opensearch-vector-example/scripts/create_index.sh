#!/bin/bash

# Set variables
OPENSEARCH_URL="https://34.139.11.141:9200"
OPENSEARCH_USER="admin"
OPENSEARCH_PASSWORD="StrongPassword123!"
INDEX_NAME="lucille_code_vectors"
MAPPING_FILE="gemini_index_mapping.json"

# Check if the mapping file exists
if [ ! -f "$MAPPING_FILE" ]; then
  echo "Error: Mapping file $MAPPING_FILE not found!"
  exit 1
fi

# Check if index exists and delete it if it does
echo "Checking if index $INDEX_NAME exists..."
HTTP_STATUS=$(curl -k -s -o /dev/null -w "%{http_code}" -u "$OPENSEARCH_USER:$OPENSEARCH_PASSWORD" "$OPENSEARCH_URL/$INDEX_NAME")

if [ $HTTP_STATUS -eq 200 ]; then
  echo "Index $INDEX_NAME already exists. Deleting..."
  curl -k -X DELETE "$OPENSEARCH_URL/$INDEX_NAME" -u "$OPENSEARCH_USER:$OPENSEARCH_PASSWORD"
  echo ""
fi

# Create the index with the mapping
echo "Creating index $INDEX_NAME with vector search mapping..."
curl -k -X PUT "$OPENSEARCH_URL/$INDEX_NAME" \
  -H 'Content-Type: application/json' \
  -u "$OPENSEARCH_USER:$OPENSEARCH_PASSWORD" \
  -d "@$MAPPING_FILE"
echo ""

# Verify the index was created
echo "Verifying index creation..."
curl -k -X GET "$OPENSEARCH_URL/$INDEX_NAME" -u "$OPENSEARCH_USER:$OPENSEARCH_PASSWORD"
echo ""

echo "Index setup complete!"
