---
title: "Project Structure and Build"
weight: 10
date: 2025-06-09
description: >
  The multi-module Maven project structure, how the build works, and how to use custom components with Lucille.
---

## Overview

Lucille is a multi-module Maven project. The module hierarchy is:

```
lucille (root aggregator)
├── lucille-parent          # parent POM: dependency versions, plugin config, build profiles
├── lucille-bom             # Bill of Materials: version-aligned dependency declarations
├── lucille-core            # the framework itself: Runner, Worker, Indexer, Pipeline, Document, Stages, Connectors
├── lucille-plugins/        # optional modules with heavy or specialized dependencies
│                           #   (tika, pinecone, weaviate, jlama, parquet, ocr, video, entity-extraction, api)
└── lucille-examples/       # runnable example projects demonstrating common ingestion patterns (not published to Maven Central)
```

---

## The Modules

### lucille-parent

The parent POM that all other modules inherit from. It defines:

- **Java version** (21)
- **Dependency versions** for all third-party libraries (Jackson, Kafka, Solr, OpenSearch, Elasticsearch, AWS SDK, etc.) as properties
- **Dependency management** section that pins versions so child modules don't need to specify versions
- **Plugin configuration** for compilation, testing, Javadoc generation, source JARs, and GPG signing
- **Build profiles** (e.g., the `deploy` profile for publishing to Maven Central with GPG signing)
- **Distribution management** pointing to Sonatype OSSRH for releases

**When should you be concerned with lucille-parent?** When you need to:
- Update a third-party dependency version (change the property in lucille-parent)
- Add a new dependency that multiple modules will use (add it to `dependencyManagement`)
- Change build plugin configuration (compiler settings, test runner, etc.)
- Prepare a release (the version number lives here)

Most day-to-day development (writing stages, connectors, tests) does not require touching lucille-parent.

### lucille-bom

The Bill of Materials POM. It declares all Lucille modules (lucille-core, lucille-pinecone, lucille-tika, etc.) with their versions aligned to `${project.version}`. External projects that depend on Lucille import the BOM in their `dependencyManagement` section, which lets them declare Lucille dependencies without specifying versions:

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
    <!-- version inherited from BOM -->
  </dependency>
  <dependency>
    <groupId>com.kmwllc</groupId>
    <artifactId>lucille-tika</artifactId>
    <!-- version inherited from BOM -->
  </dependency>
</dependencies>
```

The BOM ensures that all Lucille modules in a project are at the same version, preventing subtle incompatibilities.

### lucille-core

The framework itself. Contains:

- The core architecture: `Runner`, `Worker`, `Indexer`, `Publisher`, `Pipeline`, `Document`
- The messenger abstractions: `LocalMessenger`, `TestMessenger`, `KafkaWorkerMessenger`, etc.
- Built-in Stages (field manipulation, regex, date parsing, text operations, database enrichment, HTTP enrichment, scripting, etc.)
- Built-in Connectors (FileConnector, DatabaseConnector, SolrConnector, etc.)
- Built-in Indexers (SolrIndexer, OpenSearchIndexer, ElasticsearchIndexer, CSVIndexer)
- The SPEC validation system
- Test mode, a supported dry-run mode for pipeline development and tests (`Runner.runInTestMode`, `TestMessenger`)

This is the only module required for a minimal Lucille deployment. If your pipeline doesn't need Tika, Pinecone, OCR, etc., you only need lucille-core.

### lucille-plugins

An aggregator POM containing optional modules. Each plugin:

- Has its own `pom.xml` with `lucille-core` as a `provided` dependency
- Brings in one or more heavy third-party libraries (Tika, Tesseract, JLama, Pinecone SDK, etc.)
- Produces its own JAR artifact
- Is published independently to Maven Central

The `provided` scope on lucille-core means the plugin compiles against core but does not bundle it — at runtime, core is expected to already be on the classpath. This prevents version conflicts when multiple plugins are used together.

**When to create a new plugin module:**
- The component depends on a large library (>10MB, or with many transitive dependencies)
- The dependency is licensed in a way that not all users would accept
- The component is specialized enough that most users won't need it
- Including it in core would bloat the core JAR or cause transitive dependency conflicts

**Wiring up a new plugin module:**

A new plugin needs to be registered in these places:

- **`lucille-plugins/pom.xml`**: add the module to `<modules>`
- **`lucille-bom/pom.xml`**: add a `<dependency>` entry so consumers who import the BOM can declare the plugin without a version.
- **`lucille-all/pom.xml`**: add it if the plugin should ship in the batteries-included `lucille-all` image. Skip it for plugins that should stay opt-in.
- **`lucille-parent/pom.xml`**: add the plugin's base package to the `<includes>` list of the `api-compat` profile. That list is an explicit list of packages, so a new package is not checked for binary compatibility until it is added (see [API compatibility check](#api-compatibility-check-japicmp)).
- **`PUBLIC_API.md`**: add the plugin to the list of component plugins, and note any package that is internal.
- **Baseline override**: the compatibility check compares each module against `lucille.api.baseline.version`. A module that did not exist in baseline version has no such artifact, so the check silently skips it. Set `lucille.api.baseline.version` in that module's own `<properties>` to the first version in which the module was released, so it is guarded from then on.

### lucille-examples

Runnable example projects that demonstrate common ingestion patterns. Each example:

- Has its own `pom.xml` importing the `lucille-bom`
- Depends on `lucille-core` and whichever plugins it needs
- Includes a `conf/` directory with HOCON config files
- Often includes a `scripts/` directory with shell scripts for running
- Uses `maven-dependency-plugin` to copy runtime dependencies to `target/lib/`

Examples are **not published to Maven Central** (`<maven.deploy.skip>true</maven.deploy.skip>`). They exist purely as reference implementations and starting points for new projects.

---

## Version Management

The project version is specified in `lucille-parent/pom.xml`:

```xml
<version>0.9.0-SNAPSHOT</version>
```

All other modules inherit this version via their `<parent>` declaration. The version appears explicitly in each module's parent reference (Maven requires this), but the single source of truth is lucille-parent.

**SNAPSHOT vs. release:** During development, the version ends with `-SNAPSHOT`. When a release is cut, the version is updated to remove `-SNAPSHOT` (e.g., `0.9.0`), the release is built and published, and then the version is bumped to the next SNAPSHOT (e.g., `0.10.0-SNAPSHOT`).

**Updating the version:** Because Maven requires the version to be explicit in each module's `<parent>` block, updating the version requires changing it in every `pom.xml` in the project. This is typically done with the Maven versions plugin:

```bash
mvn versions:set -DnewVersion=0.9.0
```

`versions:set` does **not** change `lucille.api.baseline.version` in lucille-parent. That property is a fixed reference point for the API compatibility check, not a copy of the project version. 
See [API compatibility check](#api-compatibility-check-japicmp) for when and how it needs to change.

---

## How the Build Works

### Building the entire project

From the root directory:

```bash
mvn clean install
```

This builds all modules in dependency order: lucille-parent → lucille-bom → lucille-core → lucille-plugins (each plugin) → lucille-examples (each example). Each module produces a JAR artifact installed to the local Maven repository.

### Building a single module

```bash
mvn clean install -pl lucille-core
```

Or for a plugin:

```bash
mvn clean install -pl lucille-plugins/lucille-tika
```

### What artifacts are produced

- **lucille-core:** `target/lucille.jar` — a thin JAR containing only Lucille's own classes (named `lucille.jar` via `finalName` in the POM). Runtime dependencies are copied to `target/lib/`. To run Lucille, both must be on the classpath: `-cp 'target/lucille.jar:target/lib/*'` (or simply `-cp 'target/lib/*'` since the lucille JAR is also copied there).
- **Each plugin:** a **shaded (fat) JAR** that bundles the plugin's own dependencies into a single artifact. This avoids classpath conflicts when multiple plugins are used together. Plugins that produce shaded JARs include: lucille-tika, lucille-pinecone, lucille-weaviate, lucille-jlama, lucille-parquet, lucille-ocr, lucille-video, and lucille-api. The shaded JAR (`target/<plugin>-plugin.jar`) is built only when the `shaded-plugin-jars` profile is active, e.g. `mvn clean install -Pshaded-plugin-jars`; a default build produces only the plugin's thin JAR, which keeps `mvn verify` fast since tests do not use the shaded JARs.
- **Each example:** a thin JAR plus `target/lib/` containing all runtime dependencies (via `maven-dependency-plugin` copy-dependencies).

### Thin JAR vs. Shaded JAR

| Module | JAR Type | Why |
|---|---|---|
| `lucille-core` | Thin JAR + `target/lib/` | Core is always on the classpath alongside its dependencies. No need to shade. |
| Plugins (tika, pinecone, etc.) | Shaded (fat) JAR with `-Pshaded-plugin-jars` | Plugins bundle their heavyweight dependencies to avoid version conflicts with core or other plugins. A single plugin JAR can be dropped onto the classpath without worrying about transitive dependency management. |
| Examples | Thin JAR + `target/lib/` | Examples are runnable projects, not libraries. Dependencies are copied for easy classpath setup. |

**When running Lucille from the command line**, the classpath typically looks like:

```bash
java -Dconfig.file=config.conf \
  -cp 'lucille-core/target/lucille.jar:lucille-core/target/lib/*:lucille-plugins/lucille-tika/target/lucille-tika-*.jar' \
  com.kmwllc.lucille.core.Runner
```

Or more simply from an example directory where everything is in `target/lib/`:

```bash
java -Dconfig.file=conf/my-config.conf -cp 'target/lib/*' com.kmwllc.lucille.core.Runner
```

### Running an example

After building, from an example's directory:

```bash
java -Dconfig.file=conf/file-to-file-example.conf -cp 'target/lib/*' com.kmwllc.lucille.core.Runner
```

---

## Using Custom Connectors and Stages with Lucille

### Writing a custom component in your own project

If you write your own Connector or Stage in a separate Maven project, you need to:

1. **Depend on lucille-core** (and any plugins you need):

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
</dependencies>
```

2. **Implement your Stage or Connector** following the standard patterns (SPEC declaration, constructor calling `super(config)`, etc.)

3. **Build your project** to produce a JAR.

4. **Include your JAR on the classpath** when running Lucille. The simplest approach is to copy your JAR into the `target/lib/` directory alongside Lucille's JARs.

5. **Reference your class in the config** using its fully qualified class name:

```hocon
stages: [
  {
    class: "com.mycompany.lucille.stages.MyCustomStage"
    myParam: "value"
  }
]
```

Lucille instantiates Stages, Connectors, and Indexers reflectively using the `class` property in the config. As long as the class is on the classpath and follows the expected constructor signature, it will work. There is no registration step, no plugin manifest, no service loader — just put the class on the classpath and reference it by name.

### The classpath is the plugin mechanism

Lucille's "plugin system" is simply the JVM classpath. To add a capability:

1. Write a class that implements the appropriate interface or extends the appropriate base class.
2. Put the compiled class (in a JAR) on the classpath.
3. Reference it by fully qualified class name in the config.

This is why the built-in plugins are structured as separate JARs rather than using a framework-specific plugin API. A plugin JAR is just a JAR with classes that happen to extend Lucille's base classes. Your custom code works exactly the same way.

---

## Project Layout Conventions

### Source code layout

Each module follows standard Maven conventions:

```
lucille-core/
├── src/
│   ├── main/
│   │   ├── java/com/kmwllc/lucille/
│   │   │   ├── core/          # Runner, Worker, Indexer, Publisher, Pipeline, Document, Stage
│   │   │   ├── connector/     # built-in Connectors
│   │   │   ├── indexer/       # IndexerFactory and related utilities
│   │   │   ├── message/       # Messenger interfaces and implementations
│   │   │   ├── stage/         # built-in Stages
│   │   │   └── util/          # utilities (ConfigUtils, LogUtils, etc.)
│   │   └── resources/
│   │       └── log4j2.xml               # default logging configuration
│   └── test/
│       ├── java/com/kmwllc/lucille/     # test classes
│       └── resources/                    # test config files
└── pom.xml
```

### Where to put new code

- **A new general-purpose Stage** → `lucille-core/src/main/java/com/kmwllc/lucille/stage/`
- **A new Connector** → `lucille-core/src/main/java/com/kmwllc/lucille/connector/`
- **A Stage or Connector with heavy dependencies** → new module under `lucille-plugins/`
- **A new example** → new module under `lucille-examples/`
- **Test configs** → `src/test/resources/` in the relevant module

### Test JARs

lucille-core produces a test JAR (`lucille-core-0.9.0-SNAPSHOT-tests.jar`) that is available to other modules for testing. The examples module depends on this test JAR, which provides test utilities and base classes for writing integration tests against the framework.

---

## Publishing to Maven Central

Lucille is published to Maven Central via Sonatype OSSRH. The `deploy` profile in lucille-parent activates GPG signing and source/Javadoc JAR generation:

```bash
mvn clean deploy -Ddeploy
```

This publishes lucille-core, lucille-bom, and all plugin modules. Examples are excluded from publishing (`maven.deploy.skip=true`).

### API compatibility check (japicmp)

Lucille uses [japicmp](https://siom79.github.io/japicmp/) to catch accidental binary-incompatible changes to the public Java API (the packages listed in [`PUBLIC_API.md`](https://github.com/kmwtechnology/lucille/blob/main/PUBLIC_API.md)). It compares each module's freshly built jar against a released **baseline** jar and fails the build if code compiled against the baseline would no longer link against the new jar.

The check is configured in the `api-compat` profile of `lucille-parent/pom.xml`. It is **off by default**; run it with:

```bash
mvn verify -Papi-compat
```

The plugin version in use (0.26.x) needs Maven 3.8.1 or newer. On older Maven, add `-Djapicmp-maven-plugin.version=0.21.2`.

**The baseline.** The baseline version is the property `lucille.api.baseline.version` in `lucille-parent/pom.xml`. Say it is set to the 1.0 release (`1.0.0`), it is meant to stay there for the whole 1.x line. Every 1.x release is compared back to 1.0.0, so the guarantee is "code compiled against 1.0 keeps working on any later 1.x", and breaks that build up gradually over several releases are still caught.

- **Do not update it on every release.** Releases such as 1.0.1, 1.1.0 or 1.5.0 do not need any change. `mvn versions:set` leaves the property alone, which is intended: if the baseline followed the project version, a release would be compared against itself and could never fail.
- **Update it on a major version bump.** Breaking 1.x compatibility is allowed in 2.0, so when 2.0.0 is released, set the baseline to `2.0.0`. From the repository root, this updates the default in lucille-parent and any module that overrides it:

  ```bash
  mvn versions:set-property -Dproperty=lucille.api.baseline.version -DnewVersion=2.0.0
  ```

- **New modules need their own baseline.** The baseline is one version for every module. A module first released after 1.0 has no 1.0.0 artifact, so the check skips it instead of failing. Override the property in that module's own POM (see "Wiring up a new plugin module" above).

**When it takes effect.** The check needs the baseline jar to exist in a Maven repository. Until 1.0.0 is published, the baseline cannot be resolved, and the check logs a warning and passes (`ignoreMissingOldVersion`). Once 1.0.0 is released, run the release build with `-Papi-compat` so the check is enforced.

**What it checks.** Binary-incompatible changes to public and protected members of the listed packages, for example:

- a class, method, constructor or field removed
- a class, method or field made less accessible (for example public to package-private)
- a method's return type or parameter types changed (reported as a removal)
- a class or method made `final`, `abstract` or `static`, or no longer `static`
- a superclass or interface removed

**What it does not check.** It is not a full compatibility guarantee:

- *Source-only* breaks, such as adding an abstract method to an interface (binary-compatible, but implementers no longer compile)
- *Behavior* changes behind an unchanged signature
- The *HOCON configuration surface* (stage, connector and indexer property names, defaults, and `indexer.type` names). A renamed or removed property breaks users' configs without changing any Java signature, and japicmp cannot see it.
- Packages not in the `<includes>` list, and dependency changes

---

## Summary

| Module | Purpose | Published? |
|---|---|---|
| lucille-parent | Version management, dependency versions, build config | Yes |
| lucille-bom | Version-aligned dependency declarations for consumers | Yes |
| lucille-core | The framework (all core components) | Yes |
| lucille-plugins/* | Optional modules with heavy dependencies | Yes (each) |
| lucille-examples/* | Runnable reference implementations | No |

The key things a developer needs to know:

1. **lucille-core is the only required dependency.** Everything else is optional.
2. **Plugins use `provided` scope for core.** They compile against it but don't bundle it.
3. **The classpath is the plugin mechanism.** Put your JAR on the classpath, reference the class by name in config.
4. **Version is in lucille-parent.** All modules inherit it.
5. **Examples show the patterns.** Start from an example when building a new ingest project.
6. **The BOM simplifies dependency management.** Import it and you don't need to specify Lucille module versions.
7. **A new module must be registered in several places.** Add it to the plugins `<modules>`, the BOM, and the `api-compat` include list (and to `lucille-all` if it ships there).
8. **The API compatibility baseline is fixed at 1.0.0.** It changes on a major version bump, not on every release, and `versions:set` does not touch it.
