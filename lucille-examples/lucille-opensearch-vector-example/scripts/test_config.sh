#!/bin/bash

# Test the OpenSearch vector configuration
echo "Testing Lucille OpenSearch Vector Example Configuration"
echo "======================================================="

# Set up environment
source "$(dirname "$0")/setup_environment.sh"

# Extract credentials from OPENSEARCH_URL for curl commands
if [[ "$OPENSEARCH_URL" =~ https?://([^:]+):([^@]+)@([^:/]+)(:[0-9]+)? ]]; then
  USERNAME="${BASH_REMATCH[1]}"
  PASSWORD="${BASH_REMATCH[2]}"
  HOST="${BASH_REMATCH[3]}"
  PORT="${BASH_REMATCH[4]:-:9200}"
  PORT="${PORT:1}" # Remove leading colon
  CURL_URL="https://${HOST}:${PORT}"
else
  echo "❌ Could not parse credentials from OPENSEARCH_URL"
  exit 1
fi

# Check if config file exists
CONFIG_FILE="${CONFIG_FILE:-conf/opensearch-vector.conf}"
if [ ! -f "$CONFIG_FILE" ]; then
  echo "ERROR: Configuration file not found: $CONFIG_FILE"
  exit 1
fi
echo "Using configuration file: $CONFIG_FILE"

# Check if OpenSearch is running
if curl -k -s -u "${USERNAME}:${PASSWORD}" "$CURL_URL" > /dev/null; then
  echo "✅ OpenSearch is running at $CURL_URL (with authentication)"
else
  echo "❌ OpenSearch is not running or authentication failed at $CURL_URL"
  echo "   Please check that OpenSearch is running and credentials are correct"
  exit 1
fi

# Check if index exists
if curl -k -s -u "${USERNAME}:${PASSWORD}" "${CURL_URL}/${OPENSEARCH_INDEX}" > /dev/null; then
  echo "✅ Index '$OPENSEARCH_INDEX' exists"
else
  echo "⚠️ Index '$OPENSEARCH_INDEX' does not exist yet"
  echo "   Run the create_index.sh script to create it"
fi

# Validate configuration file content
echo -e "\nValidating configuration file:"
echo "-------------------------------"

# Check for GeminiEmbed stage
if grep -q "com.kmwllc.lucille.example.GeminiEmbed" "$CONFIG_FILE"; then
  echo "✅ GeminiEmbed stage is configured"
  
  # Extract source and destination fields from the GeminiEmbed section
  GEMINI_SECTION=$(sed -n '/name: "geminiEmbed"/,/}/p' "$CONFIG_FILE")
  SOURCE=$(echo "$GEMINI_SECTION" | grep "source:" | head -1 | sed -E 's/.*source:[[:space:]]*"([^"]+)".*/\1/')
  DEST=$(echo "$GEMINI_SECTION" | grep "dest:" | head -1 | sed -E 's/.*dest:[[:space:]]*"([^"]+)".*/\1/')
  
  if [ -n "$SOURCE" ] && [ -n "$DEST" ]; then
    echo "   - Embedding from field '$SOURCE' to field '$DEST'"
    # Check if the destination field matches our mapping
    if [ "$DEST" = "embedding" ]; then
      echo "   ✅ Embedding field name matches OpenSearch mapping"
    else
      echo "   ⚠️ Embedding field name '$DEST' may not match OpenSearch mapping (expected 'embedding')"
    fi
  fi
else
  echo "❌ GeminiEmbed stage is not configured in $CONFIG_FILE"
  echo "   This is required for vector embeddings"
  exit 1
fi

# Check for ChunkText stage
if grep -q "com.kmwllc.lucille.stage.ChunkText" "$CONFIG_FILE"; then
  echo "✅ ChunkText stage is configured for text chunking"
  
  # Extract chunk size and overlap
  CHUNK_SECTION=$(sed -n '/name: "textChunker"/,/}/p' "$CONFIG_FILE")
  CHUNK_SIZE=$(echo "$CHUNK_SECTION" | grep "length_to_split:" | head -1 | sed -E 's/.*length_to_split:[[:space:]]*([0-9]+).*/\1/')
  OVERLAP=$(echo "$CHUNK_SECTION" | grep "overlap_percentage:" | head -1 | sed -E 's/.*overlap_percentage:[[:space:]]*([0-9]+).*/\1/')
  
  if [ -n "$CHUNK_SIZE" ] && [ -n "$OVERLAP" ]; then
    echo "   - Chunk size: $CHUNK_SIZE, Overlap: $OVERLAP%"
  fi
else
  echo "⚠️ No ChunkText found for text chunking"
fi

# Check for required fields in the configuration
echo -e "\nChecking for required fields:"
echo "-----------------------------"

REQUIRED_FIELDS=("chunk_text" "chunk_id" "parent_id" "file_extension" "file_last_modified" "file_size_bytes")
for field in "${REQUIRED_FIELDS[@]}"; do
  if grep -q "\"$field\"" "$CONFIG_FILE"; then
    echo "✅ Field '$field' is configured"
  else
    echo "⚠️ Field '$field' may be missing from configuration"
  fi
done

# Check OpenSearch configuration
if grep -q "acceptInvalidCert" "$CONFIG_FILE"; then
  echo "✅ OpenSearch is configured to accept invalid certificates (required for self-signed certs)"
else
  echo "❌ OpenSearch is not configured to accept invalid certificates"
  echo "   Add 'acceptInvalidCert: true' to the opensearch section of $CONFIG_FILE"
  exit 1
fi

echo -e "\nConfiguration validation completed successfully! ✅"
echo "You can now run the ingest process with: ./scripts/run_ingest.sh"
