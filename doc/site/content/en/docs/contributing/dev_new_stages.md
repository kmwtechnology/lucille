---
title: Developing New Components
weight: 25
date: 2024-10-28
description: >
  The basics of how to develop Connectors, Stages, and Indexers for Lucille.
---

## Introduction
This guide covers the basics of how to develop new components for Lucille along with an understanding of required and optional components. After reading this, you should be able to start development with a good foundation on how testing works, how configuration is handled, and what features the base classes afford us. 


## Prerequisites
- An up-to-date version of Lucille that has been appropriately installed 

- Understanding of the Java programming language

- Understanding of Lucille 

## Developing Stages

### Project and Package Layout

Create your stage under:

```
lucille/lucille-core/src/main/java/com/kmwllc/lucille/stage/
```

### Stage Skeleton

Every Stage must expose a static `SPEC` that declares its config schema. Use `SpecBuilder` to define **required/optional** fields, lists, parents, and types. The base class consumes this to validate user config at load time.

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

#### Common Spec Helpers

* `requiredString/Number/Boolean` for scalar fields.
* `requiredList("field", new TypeReference<List<String>>() {})` for typed lists.
* `requiredParent("field", new TypeReference<Map<String,Object>>() {})` for nested objects.
* `optionalX(...)` variants mirror the above.
* `FileConnector.S3_PARENT_SPEC`/`AZURE_PARENT_SPEC`/`GCP_PARENT_SPEC` for cloud file parent specs.

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

#### Locations

* Tests:

```bash
lucille/lucille-core/src/test/java/com/kmwllc/lucille/stage/
```

* Per-test resources:
```bash
lucille/lucille-core/src/test/resources/<StageName>Test/
```

#### Instantiating a Stage in Tests

```java
import com.kmwllc.lucille.core.Stage;
Stage s = StageFactory.of(MyStage.class).get("MyStageTest/config.conf");
```

#### Processing a Document

```java
Document d = Document.create("doc1");
s.processDocument(d);
```

#### Testing Standards

* There should be at least one unit test for a Stage.
* Recommended to aim for 80% code coverage.
* When testing services, use mocks and spys.
* Tests should test notable exceptions thrown by Stage class code.
* Tests should cover all logical aspects of a Stage’s function.
* Tests should be deterministic.
* Code coverage should not only encapsulate aspects of lucille-core but also modules.
* Include test cases for every configuration parameter defined by the Stage (required and optional).

### Javadoc Style

Use this exact structure for class-level Javadoc:

```java
/**
 * Description can be multi‑sentence and may include details above the p tag.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>name (String, Required) : What it does.</li>
 *   <li>limit (Integer, Optional) : How it’s used. Defaults to 10.</li>
 *   <li>flags (List<String>, Optional) : Notes on values.</li>
 * </ul>
 */
```

## Developing Connectors
[TODO]

### Creating a Connector

### Unit Testing

### Extra Connector Resources

## Developing Indexers
[TODO]

### Creating an Indexer

### Unit Testing

### Extra Indexer Resources