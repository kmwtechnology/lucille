#!/bin/bash
# run this script from top level lucille-csv-example directory via ./scripts/run_ingest.sh
java -Dconfig.file=conf/file-to-file-example.conf -cp 'target/lib/*' com.kmwllc.lucille.core.Runner -local
