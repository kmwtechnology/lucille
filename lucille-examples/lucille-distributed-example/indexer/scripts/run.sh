#!/bin/bash
curl 'http://solr:8983/solr/admin/collections?action=CREATE&name=quickstart&numShards=1&collection.configName=_default'
java -Dconfig.file=/conf/main.conf -cp '/target/lib/*' com.kmwllc.lucille.core.Indexer simple_pipeline
