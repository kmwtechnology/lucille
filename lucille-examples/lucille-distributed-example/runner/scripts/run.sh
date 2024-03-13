#!/bin/sh
java -Dconfig.file=/conf/main.conf -cp '/target/lib/*' com.kmwllc.lucille.core.Runner -usekafka
# java -cp '/target/lib/*' junit.textui.TestRunner com.kmwllc.lucille.distributed.AfterTest
