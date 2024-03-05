FROM ubuntu
RUN apt-get update && apt-get install -y curl
RUN curl https://dlcdn.apache.org/zookeeper/zookeeper-3.9.1/apache-zookeeper-3.9.1-bin.tar.gz --output zookeeper.tar.gz
RUN tar -xzf zookeeper.tar.gz
RUN cd apache-zookeeper-3.9.1-bin
CMD ["bin/zookeeper-server-start.sh", "./config/zookeeper.properties"]

