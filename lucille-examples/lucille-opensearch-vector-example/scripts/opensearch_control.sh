#!/bin/bash

# Source the environment variables
source "$(dirname "$0")/setup_environment.sh"

# Check OpenSearch availability
check_opensearch() {
  echo "Checking OpenSearch availability..."
  HTTP_STATUS=$(curl -k -s -o /dev/null -w "%{http_code}" -u "admin:StrongPassword123!" "${OPENSEARCH_URL}")
  
  if [ "$HTTP_STATUS" -eq 200 ]; then
    echo "OpenSearch is accessible at ${OPENSEARCH_URL}"
    return 0
  else
    echo "OpenSearch is not accessible (status code: ${HTTP_STATUS})"
    return 1
  fi
}

# Print usage information
usage() {
  echo "Usage: $0 [check]"
  echo ""
  echo "Commands:"
  echo "  check      Check if OpenSearch is accessible"
  echo ""
}

# Main execution
case "$1" in
  check)
    check_opensearch
    ;;
  "")
    usage
    ;;
  *)
    echo "Unknown command: $1"
    usage
    exit 1
    ;;
esac
