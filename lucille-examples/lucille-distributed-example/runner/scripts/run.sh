#!/bin/sh
mkdir output
touch output/random
java -Dconfig.file=/conf/main.conf -cp '/target/lib/*' com.kmwllc.lucille.core.Runner -usekafka
# java -cp '/target/lib/*' junit.textui.TestRunner com.kmwllc.lucille.distributed.AfterTest
