#!/bin/bash
# run this script from root of this example e.g. ./scripts/run_ingest.sh
java -Dconfig.file=conf/s3-opensearch.conf -cp 'target/lib/*' com.kmwllc.lucille.core.Runner
