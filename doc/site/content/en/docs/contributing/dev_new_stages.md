---
title: Developing New Components
weight: 25
date: 2024-10-28
description: >
  The basics of how to develop Connectors, Stages, and Indexers for Lucille.
---

## Introduction
This guide covers the basics of how to develop new components for Lucille along with an understanding of required and optional components. After reading this, you should be able to start development with a good foundation on how testing works, how configuration is handled, and what features the base classes affords us. 


## Prerequisites
- An up-to-date version of Lucille that has been appropriately installed 

- Understanding of Java programming language

- Understanding of Lucille 

## Developing Stages

### Project and Package Layout

Create your stage under:

```
lucille/lucille-core/src/main/java/com/kmwllc/lucille/stage/
```

### Stage Skeleton

Every Stage must expose a static `SPEC` that declares its config schema. Use `SpecBuilder` to define **required/optional** fields, lists, parents, and types. The base class consumes this to validate user config at load time. See the [configuration]({{< relref "docs/architecture/components/Config/_index" >}}) docs for information on specs.

Every stage must follow the [Javadoc Standards](#javadoc-standards).

```java
package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.core.ConfigUtils;
import com.typesafe.config.Config;
import java.util.Iterator;
import com.kmwllc.lucille.core.Document;

/**
 * One‑line summary.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>foo (String, Required) : Description.</li>
 *   <li>bar (Integer, Optional) : Description. Defaults to 10.</li>
 * </ul>
 */
public class ExampleStage extends Stage {
  
  public static final Spec SPEC = SpecBuilder.stage()
      .requiredString("foo")
      .optionalNumber("bar")
      .build();
  
  private final String foo;
  private final int bar;

  public ExampleStage(Config config) throws StageException {
    super(config);
    this.foo = config.getString("foo");
    this.bar = ConfigUtils.getOrDefault(config, "bar", 10);
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    // mutate doc as needed
    doc.setField("out", foo + ":" + bar);
    // return null unless emitting child docs
    return null;
  }
}
```

### Lifecycle Methods

* `start()` for allocating resources and precomputing data structures.
* `processDocument(Document doc)` for transforming the current document and (optionally) returning child docs.
* `stop()` for releasing resources on shutdown.

### Reading & Writing Fields

Lucille's `Document` API supports **single-valued** and **multi-valued** fields with strong typing and convenience updaters.

**Supported types:** `String`, `Boolean`, `Integer`, `Double`, `Float`, `Long`, `Instant`, `byte[]`, `JsonNode`, `Timestamp`, `Date`.

#### Getting Values

* **Single Value:** `getString(name)`, `getInt(name)`, etc.
* **Lists:** `getStringList(name)`, `getIntList(name)`, etc.
* **Nested JSON:** `getNestedJson("a.b[2].c")` or `getNestedJson(List<Segment>)`.

#### Writing Values

* **Overwrite (single-valued):** `setField(name, value)` replaces any existing values and makes the field single valued.
* **Append (multi-valued):** `addToField(name, value)` converts to a list if needed and appends.
* **Create or append:** `setOrAdd(name, value)` creates as single-valued if missing, otherwise appends.

#### Updating Values

Use `update(name, mode, values...)`:

* `OVERWRITE`: first value overwrites, the rest append
* `APPEND`: all values append
* `SKIP`: no‑op if the field already exists

#### Nested JSON (Objects & Arrays)

* **Set:** `setNestedJson("a.b[2].c", jsonNode)` or `setNestedJson(List<Segment>, jsonNode)`.
* **Remove:** `removeNestedJson("a.b[2].c")` removes the last segment from its parent.
* **Segments:** `Document.Segment.parse("a.b[2].c")` ⇄ `Document.Segment.stringify(segments)` helps convert between string paths and structured paths.

### Unit Testing

See the [Testing Standards](#testing-standards).

## Developing Connectors

### Project and Package Layout

Create your connector under:

```
lucille/lucille-core/src/main/java/com/kmwllc/lucille/connector/
```

### Connector Skeleton

Every Connector must expose a static `SPEC` that declares its config schema. Use `SpecBuilder` to define **required/optional** fields, lists, parents, and types. The base class consumes this to validate user config at load time. See the [configuration]({{< relref "docs/architecture/components/Config/_index" >}}) docs for information on specs.

Every connector must follow the [Javadoc Standards](#javadoc-standards).

```java
package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;

/**
* One-line summary of what this Connector reads and how it emits Documents.
* <p>
* Config Parameters -
* <ul>
*   <li>sourceUri (String, Required) : Where to read from (file://, s3://, http://, etc.).</li>
*   <li>batchSize (Integer, Optional) : Max items to read before publishing a batch. Defaults to 100.</li>
* </ul>
*/
public class ExampleConnector extends AbstractConnector {

  public static final Spec SPEC = SpecBuilder.connector()
      .requiredString("sourceUri")
      .optionalNumber("batchSize")
      .build();

  private final String sourceUri;
  private final int batchSize;

  public ExampleConnector(Config config) {
    super(config);
    this.sourceUri = config.getString("sourceUri");
    this.batchSize = config.hasPath("batchSize") ? config.getInt("batchSize") : 100;
  }
  
  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    // Read from sourceUri and publish Documents.
    for (int i = 0; i < batchSize; i++) {
      Document d = Document.create(createDocId("item-" + i));
      // Populate fields on d as needed, e.g.: d.setField("source_uri", sourceUri);
      try {
        publisher.publish(d);
      } catch (Exception e) {
        throw new ConnectorException("Failed to publish document " + d.getId(), e);
      }
    }
    
  @Override
  public void close() throws ConnectorException {
    // Optional: Close network or file handlers.
  }
}
```

#### Lifecycle & Behavior Tips

* `preExecute(runId)` for preparing external connections.
* `execute(publisher)` for reading from your source and call `publisher.publish(doc)` for each `Document`.
* `postExecute(runId)` for optional cleanup or follow-up actions after `execute` completes successfully.
* `close()` for releasing resources.

### Unit Testing

See the [Testing Standards](#testing-standards).

## Developing Indexers

### Project and Package Layout

Create your indexer under:

```
lucille/lucille-core/src/main/java/com/kmwllc/lucille/indexer/
```

### Indexer Skeleton

Every Indexer must expose a static `SPEC` that declares its config schema. Use `SpecBuilder` to define **required/optional** fields, lists, parents, and types. The base class consumes this to validate user config at load time. See the [configuration]({{< relref "docs/architecture/components/Config/_index" >}}) docs for information on specs.

Every indexer must follow the [Javadoc Standards](#javadoc-standards).

```java
package com.kmwllc.lucille.indexer;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.typesafe.config.Config;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;

/**
 * One-line summary of what this Indexer does and where it sends documents. Additional details may go here as needed.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>url (String, Required) : Destination endpoint (e.g., base URL).</li>
 *   <li>index (String, Optional) : Default index/collection name. Defaults to "index1".</li>
 *   <li>batchSize (Integer, Optional) : Max docs per request. Defaults to 100.</li>
 * </ul>
 */
public class ExampleIndexer extends Indexer {
  
  public static final Spec SPEC = SpecBuilder.indexer()
      .requiredString("url")
      .optionalString("index")
      .optionalNumber("batchSize")
      .build();
  
  private final String url;
  private final String defaultIndex;
  private final int batchSize;
  
  public ExampleIndexer(Config config, IndexerMessenger messenger, String metricsPrefix, String localRunId) {
    super(config, messenger, metricsPrefix, localRunId);
    this.url = config.getString("url");
    this.defaultIndex = ConfigUtils.getOrDefault(config, "index", "index1");
    this.batchSize = ConfigUtils.getOrDefault(config, "batchSize", 100);
  }

  @Override
  public boolean validateConnection() {
    // Health check to the destination
    return true;
  }

  @Override
  protected Set<Pair<Document, String>> sendToIndex(List<Document> documents) throws Exception {
    // Send the batch using your destination client
    // Return any failed docs as pairs of (Document, reason)
    return Set.of();
  }

  @Override
  public void closeConnection() {
    // Close client resources
  }
}
```

### Lifecycle Methods

* `validateConnection()` for a quick destination availability check before starting the main loop.
* `sendToIndex(List<Document> docs)` to perform a write and return any per-document failures.
* `closeConnection()` for releasing resources on shutdown.

### Unit Testing

See the [Testing Standards](#testing-standards).

## Testing Standards

### Test Layout & Naming

* One test class per component (e.g., MyStageTest).
* Group related assertions into focused test methods with descriptive names.
* Place configs under a matching resources folder.

### Locations

* Tests:

```bash
lucille/lucille-core/src/test/java/com/kmwllc/lucille/<stage || indexer || conncetor>/
```

* Per-test resources:
```bash
lucille/lucille-core/src/test/resources/<StageName || IndexerName || ConnectorName>Test/
```

### General Testing Guidelines

* **Maximize coverage:** Aim to cover as many branches, error paths, and edge cases as practical.
* **Fast and offline:** No network or external services. Use mocks/spies only.
* **Exercise every parameter:** Ensure each parameter is covered by at least one test path.
* **Test failures:** Ensure bad configs, exceptions, empty inputs, etc. are tested.
* **Assert behavior:** Prefer testing state/interactions over log output.
* **Time:**  Avoid sleeps to prevent longer test runs.

### JaCoCo Coverage Report

* **Run:** `mvn clean install`.
* **Open report:** `lucille-core/target/jacoco-ut/index.html`.
* **Interpretation:** Focus on closing meaningful gaps in coverage and covering as much as possible.

## Javadoc Standards

The Lucille docs parser expects strict, class-level Javadoc in the following format so it can render cleanly in the UI.

**Rules:**

* Put a clear description before the `<p>` tag (can be multi-sentence).
* After `<p>`, include the literal heading `Config Parameters -` and a `<ul>` list.
* Each item must be:
  * `name (Type, Required | Optional) : Description`.
  * Use exact casing.
  * Use escape generics (e.g., List&lt;String&gt;).
* Don't add extra blank lines. Keep consistent punctuation.

**Template:**

```java
/**
 * Description of what this stage/connector/indexer does. This text can span
 * multiple sentences and be as long as you want as long as it appears before <p>.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>paramA (String, Required) : Example description.</li>
 *   <li>paramB (Integer, Optional) : Example description.</li>
 *   <li>flags (List&lt;String&gt;, Optional) : Example description.</li>
 *   <li>options (Map&lt;String, Object&gt;, Optional) : Example description.</li>
 * </ul>
 */
```