FROM ubuntu
RUN apt-get upgrade
RUN apt-get update && apt-get install curl -y
RUN apt-get install lsof bc openjdk-17-jre-headless -y --fix-missing
RUN curl -L https://www.apache.org/dyn/closer.lua/solr/solr/9.5.0/solr-9.5.0.tgz?action=download --output solr.tgz
RUN tar -xzf solr.tgz

ENTRYPOINT cd solr-9.5.0 && bin/solr start -c -force -h solr -Dsolr.jetty.host=0.0.0.0 -Dsolr.zk.embedded.host=0.0.0.0  && tail -f /dev/null
