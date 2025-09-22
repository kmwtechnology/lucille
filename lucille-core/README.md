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
```sh
cd lucille-plugins/lucille-api/
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

