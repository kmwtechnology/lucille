FROM eclipse-temurin:17

RUN apt-get update && apt-get install curl -y
RUN curl https://archive.apache.org/dist/kafka/3.7.0/kafka_2.13-3.7.0.tgz --output kafka.tgz
RUN tar -xzf kafka.tgz

HEALTHCHECK --interval=5s --timeout=3s --retries=10 --start-period=10s \
  CMD kafka_2.13-3.7.0/bin/zookeeper-shell.sh zookeeper:2181 ls / || exit 1

ENTRYPOINT kafka_2.13-3.7.0/bin/zookeeper-server-start.sh kafka_2.13-3.7.0/config/zookeeper.properties
