FROM ubuntu
RUN apt-get upgrade
RUN apt-get update && apt-get install curl -y && apt-get install maven -y
RUN apt-get install lsof bc openjdk-17-jre-headless -y --fix-missing

# we copy local build artifacts into the image that we're building
COPY target/ /target/
COPY conf/ /conf/
COPY runner/scripts/ /scripts/
COPY runner/githubpom.xml /pom.xml

RUN mkdir /output/

# we specify a command that the image should run when it launches
ENTRYPOINT chmod +x /scripts/run.sh && /scripts/run.sh && \
    curl -f 'http://solr:8983/solr/quickstart/update?commit=true' && \
    curl -f 'http://solr:8983/solr/quickstart/query?q=*:*' > /output/dest.json && \
    mvn test -DfailIfNoTests=true