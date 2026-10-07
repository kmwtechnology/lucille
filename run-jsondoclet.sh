#!/bin/bash

# Generates the stage/connector/indexer JSON docs that lucille-api serves.

PROJECT_ROOT=$(pwd)
TARGET_DIR="$PROJECT_ROOT/lucille-core/target"
LIB_DIR="$TARGET_DIR/lib"
CLASSES_DIR="$TARGET_DIR/classes"
SOURCE_DIR="$PROJECT_ROOT/lucille-core/src/main/java"
OUTPUT_DIR="$PROJECT_ROOT/lucille-plugins/lucille-api/target/classes"
ARGS_FILE="$PROJECT_ROOT/lucille-plugins/lucille-api/target/jsondoclet.args"

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

# javadoc.exe needs Windows paths and ';' separators. cygpath -m gives C:/... paths
JAVADOC_ARGS_FILE="$ARGS_FILE"
if command -v cygpath > /dev/null 2>&1; then
  CLASSPATH=$(cygpath -mp "$CLASSPATH")
  SOURCE_DIR=$(cygpath -m "$SOURCE_DIR")
  OUTPUT_DIR=$(cygpath -m "$OUTPUT_DIR")
  JAVADOC_ARGS_FILE=$(cygpath -m "$ARGS_FILE")
fi

# Shared options go in an @argfile. The classpath is too long for the Windows command line.
cat > "$ARGS_FILE" <<EOF
-doclet com.kmwllc.lucille.doclet.JsonDoclet
-docletpath "$CLASSPATH"
-classpath "$CLASSPATH"
-sourcepath "$SOURCE_DIR"
-d "$OUTPUT_DIR"
EOF

# Track failures across all three runs so a broken stage/connector doc doesn't get masked by a later success
STATUS=0

javadoc @"$JAVADOC_ARGS_FILE" -subpackages com.kmwllc.lucille.stage -o "stage-javadocs.json" || STATUS=1
javadoc @"$JAVADOC_ARGS_FILE" -subpackages com.kmwllc.lucille.connector -o "connector-javadocs.json" || STATUS=1
javadoc @"$JAVADOC_ARGS_FILE" -subpackages com.kmwllc.lucille.indexer -o "indexer-javadocs.json" || STATUS=1

if [ $STATUS -eq 0 ]; then
  echo "Documentation successfully generated"
else
  echo "Failed to generate documentation"
  exit 1
fi