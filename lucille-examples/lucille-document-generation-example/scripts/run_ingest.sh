#!/bin/bash
# run this script from root of this example e.g. ./scripts/run_ingest.sh
java -Dlog4j2.configurationFile=classpath:lucille-log4j2.xml -Dconfig.file=conf/test-documents.conf -cp 'target/lib/*' com.kmwllc.lucille.core.Runner
