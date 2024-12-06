#!/bin/bash
# run this script from top level lucille-simple-csv-solr-example directory via ./scripts/run_ingest.sh
java -Dconfig.file=conf/s3-opensearch.conf -cp 'target/lib/*' com.kmwllc.lucille.core.Runner