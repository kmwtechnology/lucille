# Lucille Core

Lucille Core is the central module of the Lucille project, providing the core ETL (Extract, Transform, Load) and search pipeline functionality. It contains the main processing engine, essential utilities, and base classes for building scalable, production-grade data ingestion and transformation workflows.

## Key Features

- **Pipeline Engine:** Orchestrates the flow of documents through configurable stages for extraction, transformation, enrichment, and loading.
- **Stage Framework:** Provides a flexible system for defining and composing processing stages, including custom logic and integrations.
- **Connector Support:** Includes connectors for popular data sources and sinks (e.g., Solr, OpenSearch, S3, file systems).
- **Utility Classes:** Offers utilities for classpath scanning, configuration, logging, and more.
- **Extensibility:** Designed to be extended by plugins and custom modules for specialized processing needs.
- **JSON Doclet:** Includes a custom Javadoc doclet (`JsonDoclet`) to generate structured JSON documentation for pipeline stages.

## Directory Structure

- `src/main/java/` — Core Java source code
- `src/assembly/` — Assembly descriptors for packaging
- `log/` — Default log output directory
- `target/` — Maven build output

## Building

To build Lucille Core and generate all artifacts:

```sh
mvn install
```

## Running API

TODO - add classpath 
TODO - add ability to do one jar ?
```sh
java -Dconfig.file=conf/simple-config.conf -jar target/lucille-api-plugin.jar server conf/api.yml

```


## Generating JSON Documentation

To generate structured JSON documentation for all pipeline stages:

```sh
mvn javadoc:javadoc
# or, using the standalone doclet:
java -cp "target/classes:$(mvn dependency:build-classpath -Dmdep.outputFile=cp.txt >/dev/null && cat cp.txt)" com.kmwllc.lucille.doclet.JsonDoclet
```

## Usage

Lucille Core is not intended to be run directly. It is used as a library by other Lucille modules and plugins, and as the foundation for custom ETL pipelines.

For examples and usage, see the `lucille-examples/` directory and the main [Lucille documentation](../README.md).

---

For more information, visit the [Lucille GitHub repository](https://github.com/kmwtechnology/lucille).


```bash

mvn dependency:copy-dependencies -DoutputDirectory=./lucille-core/target/lib


javadoc -doclet com.kmwllc.lucille.doclet.JsonDoclet \
  -docletpath "./lucille-core/target/classes:./lucille-core/target/lib/*" \
  -classpath "./lucille-core/target/classes:./lucille-core/target/lib/*" \
  -sourcepath ./lucille-core/src/main/java \
  -subpackages com.kmwllc.lucille.stage \
  -o javadocs.json \
  -d target

```

# push/sync to forked upstream
```bash
git push upstream LC-680-reflective-config-api
```

# regular push
```bash
git push origin LC-680-reflective-config-api
```

# merge from upstream main
```bash
# 1. Fetch all upstream changes
git fetch upstream

# 2. Checkout your feature branch
git checkout LC-680-reflective-config-api

# 3. Merge upstream/main into it
git merge upstream/main
```

# Testing
```bash
mvn clean test -Dtest=SequenceConnectorTest

mvn test -Dtest=SequenceConnectorTest
```


# pull from upstream LC-680-reflective-config-api
```bash
git pull upstream LC-680-reflective-config-api
```


# How to start the admin api server

* Static site (admin-ui) is published under lucille-plugins/lucille-api/src/main/resources/assets/admin
* Removed temporary CORS that I used earlier.
* Run mvn clean install -DskipTests from ../lucille-api/
* Run java -Dconfig.file=conf/simple-config.conf -jar target/lucille-api-plugin.jar server conf/api.yml
* Point your browser to http://localhost:8080/admin

# how to quick find the last commit date
git for-each-ref --sort=-committerdate --format='%(refname:short) %(committerdate)' refs/heads/


# Python Stage
mvn test -Dtest=PythonStageTest

## check if port is available
sudo netstat -tulnp | grep 25333
sudo lsof -i :25333

## Better testing
cd lucille-core
mvn test -Dtest=com.kmwllc.lucille.stage.PythonStageTest

ps aux|grep Py4jClient.py
pkill -f Py4jClient.py