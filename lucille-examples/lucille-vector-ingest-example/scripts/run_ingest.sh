#!/bin/bash
java -Dlog4j2.configurationFile=classpath:lucille-log4j2.xml -Dconfig.file=conf/10M-parquet-pinecone.conf -cp 'lib/*' com.kmwllc.lucille.core.Runner
