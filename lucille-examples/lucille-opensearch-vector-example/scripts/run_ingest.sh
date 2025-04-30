#!/bin/bash

# Exit on error
set -e

# Source the environment variables
source "$(dirname "$0")/setup_environment.sh"

# Get the directory where the script is located
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# Navigate to the parent directory
PROJECT_DIR=$(cd "$SCRIPT_DIR/../" && pwd)

cd "$PROJECT_DIR" || exit

# Set absolute path to config files
CONFIG_FILE="$PROJECT_DIR/conf/opensearch-vector.conf"
AUTH_CONFIG="$PROJECT_DIR/conf/opensearch-auth.properties"

echo "Starting Lucille ingestion process..."
echo "Indexing files from: $PROJECT_PATH"
echo "Target OpenSearch index: $OPENSEARCH_INDEX"
echo "Using OpenSearch URL: $OPENSEARCH_URL"
echo "Using config file: $CONFIG_FILE"

# Extract host and port from OPENSEARCH_URL
if [[ "$OPENSEARCH_URL" =~ https?://([^:]+):([^@]+)@([^:/]+)(:[0-9]+)? ]]; then
  USERNAME="${BASH_REMATCH[1]}"
  PASSWORD="${BASH_REMATCH[2]}"
  HOST="${BASH_REMATCH[3]}"
  PORT="${BASH_REMATCH[4]:-:9200}"
  PORT="${PORT:1}" # Remove leading colon
else
  echo "Could not parse credentials from OPENSEARCH_URL"
  exit 1
fi

# Authentication is now required with security enabled
echo "OpenSearch security is enabled, using authentication"

# Create the auth config file with HTTPS settings
echo "Creating/updating OpenSearch configuration file at $AUTH_CONFIG"
cat > "$AUTH_CONFIG" << EOF
# OpenSearch configuration - security enabled

# Core opensearch settings
opensearch.nodes.0.host=$HOST
opensearch.nodes.0.port=$PORT
opensearch.nodes.0.scheme=https

# Authentication required
opensearch.auth.username=$USERNAME
opensearch.auth.password=$PASSWORD

# SSL settings
opensearch.ssl.enabled=true
opensearch.ssl.verify=false
EOF

# Set CLASSPATH for the Java application with main JAR and lib directory
CLASSPATH="$PROJECT_DIR/target/lib/*:$PROJECT_DIR/target/lucille-opensearch-vector-example-0.5.4-SNAPSHOT.jar"

# Define Java options for OpenSearch connection with SSL trust settings
OPENSEARCH_OPTS="-Djavax.net.ssl.trustAll=true -Djavax.net.ssl.hostnameVerifier=allow"

# Generate a temporary config file with rendered environment variables
echo "Rendering configuration file with environment variables..."
java -cp "$CLASSPATH" \
  -Dconfig.file="$CONFIG_FILE" \
  -DOPENSEARCH_URL="$OPENSEARCH_URL" \
  -DOPENSEARCH_INDEX="$OPENSEARCH_INDEX" \
  -DPROJECT_PATH="$PROJECT_PATH" \
  $OPENSEARCH_OPTS \
  com.kmwllc.lucille.core.Runner \
  -render

# Run the ingestion process with increased memory allocation
echo "Starting ingestion with increased memory allocation..."
java -Xms1G -Xmx4G \
  -XX:+UseG1GC \
  -XX:MaxGCPauseMillis=200 \
  -cp "$CLASSPATH" \
  -Dconfig.file="$CONFIG_FILE" \
  -DOPENSEARCH_URL="$OPENSEARCH_URL" \
  -DOPENSEARCH_INDEX="$OPENSEARCH_INDEX" \
  -DPROJECT_PATH="$PROJECT_PATH" \
  -Dhttps.proxyHost= \
  -Dhttps.proxyPort= \
  -Dhttp.proxyHost= \
  -Dhttp.proxyPort= \
  -Djavax.net.ssl.trustAll=true \
  -Djavax.net.ssl.hostnameVerifier=allow \
  $OPENSEARCH_OPTS \
  com.kmwllc.lucille.core.Runner \
  -local

# Clean up temporary files
echo "Ingestion completed!"
echo "You can check the vectors with: ./scripts/check_vectors.sh"
