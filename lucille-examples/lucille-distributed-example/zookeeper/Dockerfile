FROM eclipse-temurin:17

RUN apt-get update && apt-get install curl -y
RUN curl https://downloads.apache.org/kafka/3.7.0/kafka_2.13-3.7.0.tgz --output kafka.tgz
RUN tar -xzf kafka.tgz

ENTRYPOINT kafka_2.13-3.7.0/bin/zookeeper-server-start.sh kafka_2.13-3.7.0/config/zookeeper.properties
