---
title: Installation
weight: 3
date: 2025-10-31
description: >
  How to get Lucille — as a Maven dependency, from a source build, or for development.
---

## Do You Need to Install Lucille?

It depends on how you're using it:

### Using Lucille as a Library in an Existing Java Project

If you're embedding Lucille inside an existing Java project (e.g., calling `Runner.run()` programmatically, or using Lucille's Document API and Stages within your own application), you don't need to install anything. Just add Lucille as a Maven dependency:

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>com.kmwllc</groupId>
      <artifactId>lucille-bom</artifactId>
      <version>1.0.0-alpha</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>

<dependencies>
  <dependency>
    <groupId>com.kmwllc</groupId>
    <artifactId>lucille-core</artifactId>
  </dependency>
  <!-- Log4j2 backend and Lucille's logging config; lucille-core logs only through the SLF4J API -->
  <dependency>
    <groupId>com.kmwllc</groupId>
    <artifactId>lucille-logging</artifactId>
    <scope>runtime</scope>
  </dependency>
  <!-- Add plugins as needed -->
  <dependency>
    <groupId>com.kmwllc</groupId>
    <artifactId>lucille-tika</artifactId>
  </dependency>
</dependencies>
```

Lucille is published to Maven Central. The BOM ensures all Lucille modules are at the same version. No source build required.

#### Core vs. Plugin Modules

`lucille-core` contains the pipeline framework, the Document/Stage/Connector/Indexer APIs, and the components that have no heavy third-party dependencies (for example the `CSV` and `Nop` indexers, as well as most stages and connectors).

Everything that pulls in a large or backend-specific dependency lives in its own **plugin module** that you add only if you want it. This keeps `lucille-core` lightweight and lets you avoid shipping, for example, the Solr client stack when you only index to OpenSearch. The search backends and vector stores are each their own module:

| Module | Provides |
|---|---|
| `lucille-solr` | `SolrIndexer`, `SolrConnector` |
| `lucille-opensearch` | `OpenSearchIndexer`, `QueryOpensearch` stage |
| `lucille-elasticsearch` | `ElasticsearchIndexer`, `ElasticsearchLookup` stage |
| `lucille-pinecone` | `PineconeIndexer` |
| `lucille-weaviate` | `WeaviateIndexer` |
| `lucille-parquet` | Parquet connector/file handler |
| `lucille-tika` | `TextExtractor` stage |
| `lucille-ocr` | `ApplyOCR` stage |
| `lucille-jlama` | `JlamaEmbed` stage |
| `lucille-entity-extraction` | `ApplyOpenNLPNameFinders` stage |

To use a component from a plugin module, add that module as a Maven dependency (the BOM manages the version):

```xml
<dependency>
  <groupId>com.kmwllc</groupId>
  <artifactId>lucille-solr</artifactId>
</dependency>
```

Then reference the component in your config. An indexer that registers a type is selected by the name of its block inside `indexer`, or by `indexer.type` or `indexer.class`:

```hocon
# By block name, resolved at runtime from the classpath. Works only when lucille-solr is on the classpath.
indexer { solr { url: "http://localhost:8983/solr" } }

# Or by fully-qualified class. This always works, and is required for indexers without a registered type.
indexer {
  class: "com.kmwllc.lucille.solr.indexer.SolrIndexer"
  solr { url: "http://localhost:8983/solr" }
}
```

Stages and connectors are always referenced by their fully-qualified `class` (e.g. `com.kmwllc.lucille.opensearch.stage.QueryOpensearch`). Each component's documentation page lists its exact `class`, module, and Maven dependency.

> **Classpath note for command-line runs:** an indexer's block name or `indexer.type` is resolved from a service file on the classpath, so the plugin JAR must be on the runtime classpath, not just a compile dependency. Plugin modules have no `target/lib/` of their own. After a source build, `lucille-all/target/lib/` contains lucille-core, lucille-logging, every plugin, and all their dependencies (see below); when you assemble a classpath yourself, make sure the plugin JAR and its dependencies are included.

### Running Lucille from the Command Line (Runner, Worker, Indexer)

If you're launching Lucille components via `java -cp ... com.kmwllc.lucille.core.Runner` (or Worker, WorkerIndexer, Indexer), you need the compiled JARs and their dependencies on the classpath. The easiest way to get these is to clone the repository and build from source:

```bash
git clone https://github.com/kmwtechnology/lucille.git
cd lucille
git checkout 1.0.0-alpha  # check out the release tag you want to use (tags have no "v" prefix)
mvn clean install
```

After the build, the JARs and dependencies are in each module's `target/` directory. You can then run Lucille with:

```bash
java -Dlog4j2.configurationFile=classpath:lucille-log4j2.xml -Dconfig.file=my-config.conf -cp 'lucille-all/target/lib/*' com.kmwllc.lucille.core.Runner
```

`lucille-all/target/lib` holds every module. For a config that uses only lucille-core components, `-cp 'lucille-core/target/lucille.jar:lucille-core/target/lib/*:lucille-logging/target/lucille-logging.jar:lucille-logging/target/lib/*'` is enough. Always include `lucille-logging` and the `-Dlog4j2.configurationFile` flag, or you get no log output.

Alternatively, skip the build and run a published Docker image: `kmwtechnology/lucille-all` (every plugin) or `kmwtechnology/lucille` (core and logging only). Images are tagged `<release>.<revision>`, for example `1.0.0-alpha.0`; there is no `latest` tag. See [Running Lucille]({{< relref "docs/getting-started/running-lucille" >}}).

### Developing New Lucille Components (Stages, Connectors, Indexers)

If you're contributing to Lucille or developing new components that will be part of the Lucille codebase, clone the repository and build from source as described below.

---

## Prerequisites

To build and run Lucille from source, you need:

* **Java 21+ JDK** (not just a JRE)
* **Maven** (recent version)

## Java Setup (JDK 21+ Required)

**Important:** Before running any Lucille commands, make sure **`JAVA_HOME` points to a JDK 21+** (not just a JRE) **and** that **`$JAVA_HOME/bin` is on your `PATH`** (or `%JAVA_HOME%\bin` on Windows). Maven and the `java` launcher rely on this.

### Verify Java

```bash
java -version
```

You should see **version 21** (or newer). If it’s missing or older than 21 install a JDK 21+ using **one** of the options below.

### Install Options

**Package manager**

* **macOS (Homebrew)**
    ```bash
    brew install openjdk@21
    ```
* **Windows (Chocolatey)**
    ```bash
    choco install microsoft-openjdk21
    ```
  
**Vendor installer**

* Download a JDK 21+ installer from a vendor such as **Oracle JDK**.
* Run the installer, then set `JAVA_HOME` as shown below.

### Set `JAVA_HOME` and `PATH`

**macOS**

```bash
export JAVA_HOME="$(/usr/libexec/java_home -v 21)"
export PATH="$JAVA_HOME/bin:$PATH"
```

**Windows**

* Open **System Properties**, **Environment Variables**.
* Create/Edit **JAVA_HOME** and point it to your JDK folder.
* Edit **Path** and add `%JAVA_HOME%\bin` above other Java entries.

## Maven Setup

```bash
mvn -v
```

You should see a recent Maven version and your Java home. If `mvn` is not found, install Maven using **one** of the options below.

### Install Options

**Package manager**

* **macOS (Homebrew)**
    ```bash
    brew install maven
    ```
* **Windows (Chocolatey)**
    ```bash
    choco install maven
    ```
  
**Binary installer**

* Download the binary zip/tar for **Apache Maven** from the official website.
* Add Maven's `bin/` to your `PATH`.

**macOS**

```bash
export PATH="<maven-dir>/bin:$PATH"
```

**Windows**

* Open **System Properties**, **Environment Variables**.
* Edit **Path** and add `<maven-dir>/bin`.


## Clone the Repository

```bash
git clone https://github.com/kmwtechnology/lucille.git
cd lucille
```

To use a specific release version, check out the corresponding tag:

```bash
git checkout 1.0.0-alpha  # replace with the version you want
```

To see available release tags:

```bash
git tag --list
```

## Build Lucille

```bash
mvn clean install
```

This compiles all modules and produces build artifacts under each module's `target/` folder. Modules and test classes are built and tested in parallel by default; to run serially, or to tell which test JVM produced a line of output, see [Controlling Test Parallelism]({{< relref "docs/developer-guide/testing#controlling-test-parallelism" >}}). After the build:

- `lucille-core/target/lucille.jar` — the core framework JAR
- `lucille-core/target/lib/` — lucille-core's runtime dependencies (no logging backend)
- `lucille-logging/target/lucille-logging.jar` and `lucille-logging/target/lib/` — the Log4j2 backend and Lucille's logging config
- `lucille-plugins/lucille-tika/target/lucille-tika-{version}.jar` — plugin JARs (one per plugin, without their dependencies)
- `lucille-all/target/lib/` — lucille-core, lucille-logging, every plugin, and all their runtime dependencies in one directory
- `lucille-examples/*/target/lib/` — example projects with all dependencies copied for easy classpath setup
