#!/bin/bash
java -Dconfig.file=conf/10M-parquet-pinecone.conf -cp 'lib/*' com.kmwllc.lucille.core.Runner
