FROM ubuntu
RUN apt-get upgrade
RUN apt-get update && apt-get install curl -y && apt-get install maven -y && apt-get install jq -y
RUN apt-get install openjdk-21-jre-headless -y --fix-missing

# we copy local build artifacts into the image that we're building
COPY target/ /target/
COPY runner/githubpom.xml /pom.xml
COPY runner/verify.sh /verify.sh
RUN chmod +x /verify.sh

RUN mkdir /output/

# Verifies the distributed run actually indexed the expected documents: polls Solr until the
# expected doc count shows up (see verify.sh), then runs the VerifyIngestResult JUnit test.
ENTRYPOINT ["/verify.sh"]
