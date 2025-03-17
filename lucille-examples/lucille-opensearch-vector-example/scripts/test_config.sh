#!/bin/bash

# Test the OpenSearch vector configuration
echo "Testing Lucille OpenSearch Vector Example Configuration"
echo "======================================================="

# Set up environment
source "$(dirname "$0")/setup_environment.sh"

# Check if config file exists
CONFIG_FILE="${CONFIG_FILE:-conf/opensearch-vector.conf}"
if [ ! -f "$CONFIG_FILE" ]; then
  echo "ERROR: Configuration file not found: $CONFIG_FILE"
  exit 1
fi
echo "Using configuration file: $CONFIG_FILE"

# Check if OpenSearch is running
if curl -s "$OPENSEARCH_URL" > /dev/null; then
  echo "✅ OpenSearch is running at $OPENSEARCH_URL"
else
  echo "❌ OpenSearch is not running at $OPENSEARCH_URL"
  echo "   Please start OpenSearch with: docker run -p 9200:9200 -p 9600:9600 -e \"discovery.type=single-node\" opensearchproject/opensearch:latest"
  exit 1
fi

# Check if index exists
if curl -s "${OPENSEARCH_URL}${OPENSEARCH_INDEX}" > /dev/null; then
  echo "✅ Index '$OPENSEARCH_INDEX' exists"
else
  echo "⚠️ Index '$OPENSEARCH_INDEX' does not exist yet"
  echo "   Run the create_index.sh script to create it"
fi

# Validate configuration file content
echo -e "\nValidating configuration file:"
echo "-------------------------------"

# Check for JlamaEmbed stage
if grep -q "com.kmwllc.lucille.jlama.stage.JlamaEmbed" "$CONFIG_FILE"; then
  echo "✅ JlamaEmbed stage is configured"
  
  # Extract model name
  MODEL=$(grep "model" "$CONFIG_FILE" | grep -v "#" | head -1 | sed -E 's/.*model[[:space:]]*=[[:space:]]*"([^"]+)".*/\1/')
  if [ -n "$MODEL" ]; then
    echo "   - Using model: $MODEL"
  fi
  
  # Extract source and destination fields
  SOURCE=$(grep "source" "$CONFIG_FILE" | grep -v "#" | head -1 | sed -E 's/.*source[[:space:]]*=[[:space:]]*"([^"]+)".*/\1/')
  DEST=$(grep "dest" "$CONFIG_FILE" | grep -v "#" | head -1 | sed -E 's/.*dest[[:space:]]*=[[:space:]]*"([^"]+)".*/\1/')
  if [ -n "$SOURCE" ] && [ -n "$DEST" ]; then
    echo "   - Embedding from field '$SOURCE' to field '$DEST'"
  fi
else
  echo "❌ JlamaEmbed stage is not configured in $CONFIG_FILE"
  exit 1
fi

# Check for SplitField stage
if grep -q "com.kmwllc.lucille.core.SplitField" "$CONFIG_FILE"; then
  echo "✅ SplitField stage is configured for text chunking"
else
  echo "⚠️ No SplitField found for text chunking"
fi

# Check OpenSearch configuration
if grep -q "com.kmwllc.lucille.opensearch.OpenSearchConnector" "$CONFIG_FILE"; then
  echo "✅ OpenSearch connector is configured"
else
  echo "❌ OpenSearch connector is not configured"
  exit 1
fi

echo -e "\nConfiguration validation completed successfully! ✅"
echo "You can now run the ingest process with: ./scripts/run_ingest.sh"
