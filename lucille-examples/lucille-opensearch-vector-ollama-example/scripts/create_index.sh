#!/bin/bash

# Set variables (using values from environment variables)
INDEX_NAME="$OPENSEARCH_INDEX"
MAPPING_FILE="$(dirname "$0")/../mapping/opensearch_vector_mappings.json"

# Extract credentials from OPENSEARCH_URL for curl commands
CURL_URL="$OPENSEARCH_URL"

# Check if the mapping file exists
if [ ! -f "$MAPPING_FILE" ]; then
  echo "Error: Mapping file $MAPPING_FILE not found!"
  exit 1
fi

# Check if index exists and delete it if it does
echo "Checking if index $INDEX_NAME exists..."
HTTP_STATUS=$(curl -k -s -o /dev/null -w "%{http_code}" "${CURL_URL}/${INDEX_NAME}")

if [ $HTTP_STATUS -eq 200 ]; then
  echo "Index $INDEX_NAME already exists. Deleting..."
  curl -k -X DELETE "${CURL_URL}/${INDEX_NAME}"
  echo ""
fi

# Create the index with the mapping
echo "Creating index $INDEX_NAME with vector search mapping..."
curl -k -X PUT "${CURL_URL}/${INDEX_NAME}" \
  -H 'Content-Type: application/json' \
  -d "@$MAPPING_FILE"
echo ""

# Verify the index was created
echo "Verifying index creation..."
curl -k -X GET $AUTH_PARAM "${CURL_URL}/${INDEX_NAME}"
echo ""

echo "Index setup complete!"
