#!/bin/bash

# Script to control OpenSearch and OpenSearch Dashboards with Docker Compose

# Get the directory where the script is located
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# Navigate to the parent directory
PROJECT_DIR=$(cd "$SCRIPT_DIR/.." && pwd)

cd "$PROJECT_DIR" || exit

# Function to display usage information
show_usage() {
  echo "Usage: $0 [start|stop|status|restart|logs|reset]"
  echo ""
  echo "Options:"
  echo "  start   - Start OpenSearch and OpenSearch Dashboards containers"
  echo "  stop    - Stop the containers"
  echo "  status  - Check if containers are running"
  echo "  restart - Restart the containers"
  echo "  logs    - Show logs from containers"
  echo "  reset   - Stop containers and remove volumes (WARNING: ALL DATA WILL BE LOST)"
  echo ""
  echo "Examples:"
  echo "  $0 start"
  echo "  $0 logs"
}

# Check if Docker is running
check_docker() {
  if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running or not installed."
    echo "Please make sure Docker is installed and running."
    exit 1
  fi
}

# Start the containers
start_containers() {
  echo "Starting OpenSearch and OpenSearch Dashboards..."
  docker-compose up -d
  echo ""
  echo "Waiting for OpenSearch to be ready..."
  
  # Wait for OpenSearch to be ready
  timeout=90
  counter=0
  while ! curl -s http://localhost:9200 > /dev/null; do
    counter=$((counter+1))
    if [ $counter -gt $timeout ]; then
      echo "Timeout waiting for OpenSearch to start"
      exit 1
    fi
    printf "."
    sleep 1
  done
  
  echo ""
  echo "OpenSearch is running at: http://localhost:9200"
  echo "OpenSearch Dashboards will be available at: http://localhost:5601"
  echo ""
  echo "Environment setup:"
  echo "export OPENSEARCH_URL=http://localhost:9200"
  echo "export OPENSEARCH_INDEX=lucille_code_vectors"
}

# Stop the containers
stop_containers() {
  echo "Stopping OpenSearch and OpenSearch Dashboards..."
  docker-compose down
}

# Check status of containers
check_status() {
  echo "Checking status of OpenSearch containers..."
  docker-compose ps
}

# Show logs from containers
show_logs() {
  echo "Showing logs from OpenSearch containers..."
  docker-compose logs $1
}

# Reset everything (remove volumes)
reset_containers() {
  echo "WARNING: This will remove all data in OpenSearch!"
  read -p "Are you sure you want to continue? (y/n): " -n 1 -r
  echo ""
  if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Removing containers and volumes..."
    docker-compose down -v
    echo "All OpenSearch data has been removed."
  else
    echo "Operation canceled."
  fi
}

# Main logic
check_docker

case "$1" in
  start)
    start_containers
    ;;
  stop)
    stop_containers
    ;;
  status)
    check_status
    ;;
  restart)
    stop_containers
    start_containers
    ;;
  logs)
    show_logs $2
    ;;
  reset)
    reset_containers
    ;;
  *)
    show_usage
    ;;
esac
