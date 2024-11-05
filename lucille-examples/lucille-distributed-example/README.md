## How to run the example

1. Run ```mvn clean install -DskipTests``` at the top level directory of lucille to build the project.
2. Run ```mvn verify -Pnightly``` at the top in the ```lucille-distributed-example``` directory to run the distributed example.

### What the mvn verify -Pnightly command does

1. It runs the nightly profile defined in the pom.xml. It runs an exec(docker compose up with --abort-on-container-exit), runs a test within the runner container and exits with the exit code of the runner container.
2. The docker-compose.yaml spins up lucille in distributed mode, and runs 2 different lucille runs.
3. After both the runs have completed, the runner container runs a verification test to make sure that all documents have been indexed and are searchable.
4. If the test passes, the runner container exits, which closes all containers and maven verify succeeds.
5. If the test fails, the runner container exits with a status exit code 1, which causes mvn verify to fail.

Note: running ```docker-compose up --abort-on-container-exit``` by itself will not verify the test results.