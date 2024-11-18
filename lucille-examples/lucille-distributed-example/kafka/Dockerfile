FROM eclipse-temurin:17

RUN apt-get update && apt-get install curl -y
RUN curl https://archive.apache.org/dist/kafka/3.7.0/kafka_2.13-3.7.0.tgz --output kafka.tgz
RUN tar -xzf kafka.tgz

RUN apt-get install kafkacat -y 

COPY kafka/server.properties kafka_2.13-3.7.0/config/server.properties

ENTRYPOINT kafka_2.13-3.7.0/bin/kafka-server-start.sh kafka_2.13-3.7.0/config/server.properties
