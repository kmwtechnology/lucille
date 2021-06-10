## File Traverser: Manual Test Setup

Start Kafka, create test topic, and start a console consumer:

    <kafka-install>/bin/zookeeper-server-start.sh ./config/zookeeper.properties
    <kafka-install>/bin/kafka-server-start.sh ./config/server.properties
    <kafka_install>/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 2 --topic traverser_test
    <kafka_install>/bin/kafka-console-consumer.sh --topic traverser_test --bootstrap-server localhost:9092 -- property print.key=true


Create some test files:

    mkdir /tmp/traverser
    echo "Test file 1 contents" > /tmp/traverser/file1.txt
    echo "Test file 2 contents" > /tmp/traverser/file2.txt

Build Lucille:

    lucille$ mvn clean install


Run traverser:

    lucille$ java -cp target/lucille-bundled-0.1.jar com.kmwllc.lucille.producer.FileTraverser -b -i ".*\\.txt" -p /tmp/traverser/ -l localhost:9092 -t traverser_test

Look for output from Kafka Console Consumer similar to this:

    {"id":"Yvy9Y8zwPPGCD1GxM36/1Q==","file_path":"/tmp/traverser/file2.txt","file_modification_date":"2021-06-10T20:00:01.143502Z","file_creation_date":"2021-06-10T20:00:01Z","file_size_bytes":21,"file_content":"VGVzdCBmaWxlIDIgY29udGVudHMK"}
    {"id":"cFkL8raEaubWEgDPDZE6AA==","file_path":"/tmp/traverser/file1.txt","file_modification_date":"2021-06-10T19:59:48.999526Z","file_creation_date":"2021-06-10T19:59:48Z","file_size_bytes":21,"file_content":"VGVzdCBmaWxlIDEgY29udGVudHMK"}
