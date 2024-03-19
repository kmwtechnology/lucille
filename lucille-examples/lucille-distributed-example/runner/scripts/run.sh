#!/bin/sh
java -Dconfig.file=/conf/main.conf -cp '/target/lib/*' com.kmwllc.lucille.core.Runner -usekafka
# java -cp '/target/lucille-distributed-example-0.2.0-SNAPSHOT.jar:/target/lib/*' com.kmwllc.lucille.distributed.QuerySolr
curl 'http://solr:8983/solr/quickstart/update?commit=true'
curl 'http://solr:8983/solr/quickstart/query?q=*:*' >/output/dest.json
