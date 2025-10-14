#!/bin/sh
java -Dconfig.file=/conf/main.conf -cp '/target/lib/*' com.kmwllc.lucille.core.Runner -useKafka
