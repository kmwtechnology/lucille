#!/bin/bash

# Exit on error
set -e

# Source the environment variables
source "$(dirname "$0")/setup_environment.sh"

# Get the directory where the script is located
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# Navigate to the parent directory
PROJECT_DIR=$(cd "$SCRIPT_DIR/.." && pwd)

cd "$PROJECT_DIR" || exit

# Set absolute path to config file
CONFIG_FILE="$PROJECT_DIR/conf/opensearch-vector.conf"

echo "Starting Lucille ingestion process..."
echo "Indexing files from: $PROJECT_PATH"
echo "Target OpenSearch index: $OPENSEARCH_INDEX"
echo "Using config file: $CONFIG_FILE"

# Make sure the classpath includes all necessary JARs
CLASSPATH="$PROJECT_DIR/target/lib/*:$PROJECT_DIR/target/lucille-opensearch-vector-example-0.5.4-SNAPSHOT.jar"

# First render the config file to see if variable substitution is working
echo "Rendering config file to check variable substitution..."
java -cp "$CLASSPATH" \
  -Dconfig.file="$CONFIG_FILE" \
  com.kmwllc.lucille.core.Runner \
  -render

# Run the ingestion process
echo "Starting ingestion..."
java -cp "$CLASSPATH" \
  -Dconfig.file="$CONFIG_FILE" \
  com.kmwllc.lucille.core.Runner \
  -local

echo "Ingestion completed!"
echo "You can check the vectors with: ./check_vectors.sh"
