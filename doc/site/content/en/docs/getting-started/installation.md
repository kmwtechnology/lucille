---
title: Installation
weight: 2
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
      <version>0.9.0</version>
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
  <!-- Add plugins as needed -->
  <dependency>
    <groupId>com.kmwllc</groupId>
    <artifactId>lucille-tika</artifactId>
  </dependency>
</dependencies>
```

Lucille is published to Maven Central. The BOM ensures all Lucille modules are at the same version. No source build required.

### Running Lucille from the Command Line (Runner, Worker, Indexer)

If you're launching Lucille components via `java -cp ... com.kmwllc.lucille.core.Runner` (or Worker, WorkerIndexer, Indexer), you need the compiled JARs and their dependencies on the classpath. The easiest way to get these is to clone the repository and build from source:

```bash
git clone https://github.com/kmwtechnology/lucille.git
cd lucille
git checkout v0.9.0  # check out the release tag/branch you want to use
mvn clean install
```

After the build, the JARs and dependencies are in each module's `target/` directory. You can then run Lucille with:

```bash
java -Dconfig.file=my-config.conf -cp 'lucille-core/target/lucille-core-0.9.0.jar:lucille-core/target/lib/*' com.kmwllc.lucille.core.Runner
```

### Developing New Lucille Components (Stages, Connectors, Indexers)

If you're contributing to Lucille or developing new components that will be part of the Lucille codebase, clone the repository and build from source as described below.

---

## Prerequisites

To build and run Lucille from source, you need:

* **Java 17+ JDK** (not just a JRE)
* **Maven** (recent version)

## Java Setup (JDK 17+ Required)

**Important:** Before running any Lucille commands, make sure **`JAVA_HOME` points to a JDK 17+** (not just a JRE) **and** that **`$JAVA_HOME/bin` is on your `PATH`** (or `%JAVA_HOME%\bin` on Windows). Maven and the `java` launcher rely on this.

### Verify Java

```bash
java -version
```

You should see **version 17** (or newer). If it’s missing or older than 17 install a JDK 17+ using **one** of the options below.

### Install Options

**Package manager**

* **macOS (Homebrew)**
    ```bash
    brew install openjdk@17
    ```
* **Windows (Chocolatey)**
    ```bash
    choco install microsoft-openjdk17
    ```
  
**Vendor installer**

* Download a JDK 17+ installer from a vendor such as **Oracle JDK**.
* Run the installer, then set `JAVA_HOME` as shown below.

### Set `JAVA_HOME` and `PATH`

**macOS**

```bash
export JAVA_HOME="$(/usr/libexec/java_home -v 17)"
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
git checkout 0.9.0  # replace with the version you want
```

To see available release tags:

```bash
git tag --list
```

## Build Lucille

```bash
mvn clean install
```

This compiles all modules and produces build artifacts under each module's `target/` folder. After the build:

- `lucille-core/target/lucille-core-{version}.jar` — the core framework JAR
- `lucille-core/target/lib/` — all runtime dependencies
- `lucille-plugins/lucille-tika/target/lucille-tika-{version}.jar` — plugin JARs (one per plugin)
- `lucille-examples/*/target/lib/` — example projects with all dependencies copied for easy classpath setup
