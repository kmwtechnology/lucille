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

# Verify authentication credentials
if [[ -z "$OPENSEARCH_USERNAME" || -z "$OPENSEARCH_PASSWORD" ]]; then
  echo "Error: OpenSearch authentication credentials not set."
  echo "Please ensure OPENSEARCH_USERNAME and OPENSEARCH_PASSWORD are set in setup_environment.sh"
  exit 1
else
  echo "OpenSearch authentication: $OPENSEARCH_USERNAME:********"
fi

# Create base64 encoded auth string
AUTH_BASE64=$(echo -n "${OPENSEARCH_USERNAME}:${OPENSEARCH_PASSWORD}" | base64)
echo "Generated Base64 auth token: ${AUTH_BASE64}"

# Create the auth config file with direct credential format
echo "Creating/updating OpenSearch authentication configuration file at $AUTH_CONFIG"
cat > "$AUTH_CONFIG" << EOF
# OpenSearch authentication configuration - direct settings with raw values

# Core opensearch settings
opensearch.nodes.0.host=34.23.62.171
opensearch.nodes.0.port=9200
opensearch.nodes.0.scheme=https
opensearch.nodes.0.username=admin
opensearch.nodes.0.password=StrongPassword123!

# Authentication settings
opensearch.security.user=admin
opensearch.security.password=StrongPassword123!
opensearch.security.enabled=true

# Connection settings
opensearch.http.ssl=true
opensearch.ssl.verification_mode=none
opensearch.ssl.verification=false
opensearch.verify_ssl=false

# Header format for direct auth
opensearch.authorization=Basic ${AUTH_BASE64}

# Standard form
opensearch.username=admin
opensearch.password=StrongPassword123!
opensearch.auth.type=basic
EOF

# Create a direct environment variable for HTTP headers
echo "Creating direct system properties and environment variables"

# Create a direct authentication Java properties file
AUTH_PROPS=$(mktemp)
echo "Creating auth properties file at: $AUTH_PROPS"
cat > "$AUTH_PROPS" << EOF
org.opensearch.client.restclient.auth.enabled=true
org.opensearch.client.restclient.auth.username=admin
org.opensearch.client.restclient.auth.password=StrongPassword123!
org.opensearch.client.restclient.ssl.enabled=true
org.opensearch.client.restclient.ssl.verify=false
org.opensearch.acceptInvalidCert=true
EOF

# Make sure the classpath includes all necessary JARs
CLASSPATH="$PROJECT_DIR/target/lib/*:$PROJECT_DIR/target/lucille-opensearch-vector-example-0.5.4-SNAPSHOT.jar"

# First try a simple curl test to see if we can connect to OpenSearch
echo "Testing OpenSearch connection with curl..."
curl -k -u "$OPENSEARCH_USERNAME:$OPENSEARCH_PASSWORD" -X GET "$OPENSEARCH_URL" || echo "Curl test failed, but continuing with Java client"

# Add a curl check with explicit headers
echo "Testing OpenSearch connection with explicit Authorization header..."
curl -k -H "Authorization: Basic ${AUTH_BASE64}" -X GET "$OPENSEARCH_URL" || echo "Curl test with explicit header failed, but continuing with Java client"

# First render the config file to see if variable substitution is working
echo "Rendering config file to check variable substitution..."
OPENSEARCH_OPTS="-Dopensearch.config.file=$AUTH_PROPS \
  -Djavax.net.ssl.trustStore=/dev/null \
  -Djavax.net.ssl.trustStorePassword=password \
  -Djavax.net.ssl.trustStoreType=JKS \
  -Dsun.net.http.allowRestrictedHeaders=true \
  -Dhttps.protocols=TLSv1.2 \
  -Dorg.apache.http.auth.protocol.http.enabled=true \
  -Dorg.apache.http.auth.protocol.https.enabled=true \
  -Dopensearch.client.authentication.enabled=true \
  -Dopensearch.acceptInvalidCert=true"

export OPENSEARCH_OPTS
export OPENSEARCH_JAVA_OPTS="$OPENSEARCH_OPTS -Xms1G -Xmx4G"
export HTTP_AUTHORIZATION="Basic ${AUTH_BASE64}"
export OPENSEARCH_PATH_CONF="$PROJECT_DIR/conf"
export _JAVA_OPTIONS="$OPENSEARCH_OPTS"

java -cp "$CLASSPATH" \
  -Dconfig.file="$CONFIG_FILE" \
  -DOPENSEARCH_URL="$OPENSEARCH_URL" \
  -DOPENSEARCH_USERNAME="$OPENSEARCH_USERNAME" \
  -DOPENSEARCH_PASSWORD="$OPENSEARCH_PASSWORD" \
  -DOPENSEARCH_INDEX="$OPENSEARCH_INDEX" \
  -DPROJECT_PATH="$PROJECT_PATH" \
  -Dauthentication.method=Basic \
  -Dauthentication.header="Basic ${AUTH_BASE64}" \
  -DHTTP_AUTHORIZATION="Basic ${AUTH_BASE64}" \
  -Dhttps.proxyHost= \
  -Dhttps.proxyPort= \
  -Dhttp.proxyHost= \
  -Dhttp.proxyPort= \
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
  -DOPENSEARCH_USERNAME="$OPENSEARCH_USERNAME" \
  -DOPENSEARCH_PASSWORD="$OPENSEARCH_PASSWORD" \
  -DOPENSEARCH_INDEX="$OPENSEARCH_INDEX" \
  -DPROJECT_PATH="$PROJECT_PATH" \
  -Dauthentication.method=Basic \
  -Dauthentication.header="Basic ${AUTH_BASE64}" \
  -DHTTP_AUTHORIZATION="Basic ${AUTH_BASE64}" \
  -Dhttps.proxyHost= \
  -Dhttps.proxyPort= \
  -Dhttp.proxyHost= \
  -Dhttp.proxyPort= \
  $OPENSEARCH_OPTS \
  com.kmwllc.lucille.core.Runner \
  -local

# Clean up temporary files
rm -f "$AUTH_PROPS"
echo "Removed temporary auth properties file"

echo "Ingestion completed!"
echo "You can check the vectors with: ./scripts/check_vectors.sh"
