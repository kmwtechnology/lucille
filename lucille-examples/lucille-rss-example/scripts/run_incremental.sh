#!/bin/bash
# run this script from top level lucille-rss-example directory via ./scripts/run_incremental.sh
java -Dlog4j2.configurationFile=classpath:lucille-log4j2.xml -Dconfig.file=conf/incremental.conf -cp 'target/lib/*' com.kmwllc.lucille.core.Runner