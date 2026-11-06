#!/bin/bash
# run from the lucille-docling-example directory via ./scripts/run_ingest.sh
java -Dconfig.file=conf/docling-example.conf -cp 'target/lib/*' com.kmwllc.lucille.core.Runner
