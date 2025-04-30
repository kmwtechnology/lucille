#!/bin/bash

# Source environment variables
source "$(dirname "$0")/setup_environment.sh"

# Set variables (using values from environment variables)
INDEX_NAME="$OPENSEARCH_INDEX"
MAPPING_FILE="$(dirname "$0")/../mapping/opensearch_vector_mappings.json"

# Extract credentials from OPENSEARCH_URL for curl commands
if [[ "$OPENSEARCH_URL" =~ https?://([^:]+):([^@]+)@([^:/]+)(:[0-9]+)? ]]; then
  USERNAME="${BASH_REMATCH[1]}"
  PASSWORD="${BASH_REMATCH[2]}"
  HOST="${BASH_REMATCH[3]}"
  PORT="${BASH_REMATCH[4]:-:9200}"
  PORT="${PORT:1}" # Remove leading colon
  CURL_URL="https://${HOST}:${PORT}"
  AUTH_PARAM="-u ${USERNAME}:${PASSWORD}"
else
  CURL_URL="$OPENSEARCH_URL"
  AUTH_PARAM=""
fi

# Check if the mapping file exists
if [ ! -f "$MAPPING_FILE" ]; then
  echo "Error: Mapping file $MAPPING_FILE not found!"
  exit 1
fi

# Check if index exists and delete it if it does
echo "Checking if index $INDEX_NAME exists..."
HTTP_STATUS=$(curl -k -s -o /dev/null -w "%{http_code}" $AUTH_PARAM "${CURL_URL}/${INDEX_NAME}")

if [ $HTTP_STATUS -eq 200 ]; then
  echo "Index $INDEX_NAME already exists. Deleting..."
  curl -k -X DELETE $AUTH_PARAM "${CURL_URL}/${INDEX_NAME}"
  echo ""
fi

# Create the index with the mapping
echo "Creating index $INDEX_NAME with vector search mapping..."
curl -k -X PUT $AUTH_PARAM "${CURL_URL}/${INDEX_NAME}" \
  -H 'Content-Type: application/json' \
  -d "@$MAPPING_FILE"
echo ""

# Verify the index was created
echo "Verifying index creation..."
curl -k -X GET $AUTH_PARAM "${CURL_URL}/${INDEX_NAME}"
echo ""

echo "Index setup complete!"
