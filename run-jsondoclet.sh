#!/bin/bash

# Directory paths
PROJECT_ROOT=$(pwd)
TARGET_DIR="$PROJECT_ROOT/lucille-core/target"
LIB_DIR="$TARGET_DIR/lib"
CLASSES_DIR="$TARGET_DIR/classes"
OUTPUT_DIR="$PROJECT_ROOT/lucille-plugins/lucille-api/target/classes"

# Ensure output directory exists
mkdir -p "$OUTPUT_DIR"

# Make sure the lib directory exists
if [ ! -d "$LIB_DIR" ]; then
  echo "Library directory not found at $LIB_DIR. Running Maven to download dependencies..."
  mvn dependency:copy-dependencies -DoutputDirectory="$LIB_DIR" -f "$PROJECT_ROOT/lucille-core/pom.xml"
fi

# Build the classpath with all dependencies
CLASSPATH="$CLASSES_DIR"

# Add all JARs in the lib directory
for JAR in "$LIB_DIR"/*.jar; do
  CLASSPATH="$CLASSPATH:$JAR"
done

# Run javadoc with the JsonDoclet
# stages
javadoc \
  -doclet com.kmwllc.lucille.doclet.JsonDoclet \
  -docletpath "$CLASSPATH" \
  -classpath "$CLASSPATH" \
  -sourcepath "$PROJECT_ROOT/lucille-core/src/main/java" \
  -subpackages com.kmwllc.lucille.stage \
  -o "stage-javadocs.json" \
  -d "$OUTPUT_DIR"

# connectors
javadoc \
  -doclet com.kmwllc.lucille.doclet.JsonDoclet \
  -docletpath "$CLASSPATH" \
  -classpath "$CLASSPATH" \
  -sourcepath "$PROJECT_ROOT/lucille-core/src/main/java" \
  -subpackages com.kmwllc.lucille.connector \
  -o "connector-javadocs.json" \
  -d "$OUTPUT_DIR"

# indexers
javadoc \
  -doclet com.kmwllc.lucille.doclet.JsonDoclet \
  -docletpath "$CLASSPATH" \
  -classpath "$CLASSPATH" \
  -sourcepath "$PROJECT_ROOT/lucille-core/src/main/java" \
  -subpackages com.kmwllc.lucille.indexer \
  -o "indexer-javadocs.json" \
  -d "$OUTPUT_DIR"


# Check if the command was successful
if [ $? -eq 0 ]; then
  echo "Documentation successfully generated"
else
  echo "Failed to generate documentation"
  exit 1
fi