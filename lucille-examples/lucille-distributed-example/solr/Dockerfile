FROM ubuntu
RUN apt-get upgrade
RUN apt-get update && apt-get install curl -y
RUN apt-get install lsof bc openjdk-21-jre-headless -y --fix-missing
RUN curl -L https://www.apache.org/dyn/closer.lua/solr/solr/10.0.0/solr-10.0.0.tgz?action=download --output solr.tgz
RUN tar -xzf solr.tgz

ENTRYPOINT cd solr-10.0.0 && bin/solr start --force --host solr -Dsolr.host.bind=0.0.0.0 -Dsolr.zookeeper.embedded.host=0.0.0.0  && tail -f /dev/null
