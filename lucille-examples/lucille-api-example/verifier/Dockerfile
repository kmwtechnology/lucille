# Runs VerifyIngestResult against the api container. Built from the example's root (see docker-compose.yml)
# so it can copy the compiled test classes and the jars they need, which the example's build puts in target/.
FROM eclipse-temurin:21-jre

COPY target/test-classes/ /verifier/classes/
COPY target/verifier-lib/ /verifier/lib/
COPY verifier/verification.sh /verification.sh
RUN chmod +x /verification.sh

# Waits for the API to be ready (see verification.sh), then runs the VerifyIngestResult JUnit test.
ENTRYPOINT ["/verification.sh"]
