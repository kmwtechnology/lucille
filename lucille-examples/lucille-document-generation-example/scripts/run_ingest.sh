#!/bin/bash
# run this script from root of this example e.g. ./scripts/run_ingest.sh
java -Dconfig.file=conf/test-documents.conf -cp 'target/lib/*' com.kmwllc.lucille.core.Runner
