#!/bin/bash
# run this script from top level lucille-rss-example directory via ./scripts/run_ingest.sh
java -Dconfig.file=conf/single.conf -cp 'target/lib/*' com.kmwllc.lucille.core.Runner