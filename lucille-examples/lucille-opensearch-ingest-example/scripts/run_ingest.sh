#!/bin/bash
# run this script from top level lucille-simple-csv-solr-example directory via ./scripts/run_ingest.sh
java -Dconfig.file=conf/opensearch.conf -cp 'target/lib/*' com.kmwllc.lucille.core.Runner