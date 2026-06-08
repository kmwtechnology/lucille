---
weight: 50
title: Testing Pipelines
date: 2025-06-09
description: How to write integration tests for Lucille pipelines using RunType.TEST and the TestMessenger infrastructure.
---

Lucille provides a first-class test mode that lets you run a complete pipeline end-to-end against real source data, without needing a running search backend. All documents, events, and messages are captured in memory and available for assertion after the run.

## RunType.TEST

When a run is started with `RunType.TEST`, Lucille:

- Runs all components (Connector, Workers, Indexer) as normal.
- **Bypasses the search backend** — no actual indexing occurs.
- **Captures all messages** flowing between components in a `TestMessenger`.
- Returns a `RunResult` containing the captured message history for assertions.

## Running in Test Mode

Use `Runner.runInTestMode(config)` or construct a `Runner` with `RunType.TEST`:

```java
import com.kmwllc.lucille.core.Runner;
import com.kmwllc.lucille.core.RunResult;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@Test
public void testMyPipeline() throws Exception {
    Config config = ConfigFactory.load("my-pipeline-test.conf");
    RunResult result = Runner.runInTestMode(config);

    assertTrue("Run should succeed", result.isSuccess());
}
```

## Asserting on Documents

After the run, you can inspect what documents were sent for processing and what reached the (bypassed) Indexer:

```java
RunResult result = Runner.runInTestMode(config);

// Documents the connector published (sent for processing by Workers)
List<Document> published = result.getDocsSentForProcessing("my-connector");

// Documents that completed the pipeline (sent for indexing)
List<Document> indexed = result.getDocsSentForIndexing("my-connector");

assertEquals(100, published.size());
assertEquals(95, indexed.size());  // 5 were dropped

// Inspect individual documents
Document first = indexed.get(0);
assertEquals("expected-value", first.getString("my_field"));
assertTrue(first.has("enriched_field"));
```

## Asserting on Events

```java
List<Event> events = result.getEvents("my-connector");

long failCount = events.stream()
    .filter(e -> e.getType() == Event.Type.FAIL)
    .count();
assertEquals(0, failCount);

long dropCount = events.stream()
    .filter(e -> e.getType() == Event.Type.DROP)
    .count();
assertEquals(5, dropCount);
```

## Testing a Single Stage

For unit tests that don't need a full pipeline run, test a Stage directly:

```java
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.stage.RenameFields;
import com.typesafe.config.ConfigFactory;

@Test
public void testRenameFields() throws Exception {
    Config config = ConfigFactory.parseString(
        "class: \"com.kmwllc.lucille.stage.RenameFields\"\n" +
        "fieldMapping: { old_name: new_name }"
    );

    RenameFields stage = new RenameFields(config);
    stage.start();

    Document doc = Document.create("test-1");
    doc.setField("old_name", "hello");

    stage.processDocument(doc);

    assertFalse(doc.has("old_name"));
    assertEquals("hello", doc.getString("new_name"));

    stage.stop();
}
```

## Test Layout and Locations

One test class per component (e.g., `MyStageTest` for `MyStage`). Group related assertions into focused test methods with descriptive names.

Test classes and their resources live under `lucille-core/src/test/`:

```
src/test/java/com/kmwllc/lucille/
  stage/          ← Stage test classes
  connector/      ← Connector test classes
  indexer/        ← Indexer test classes

src/test/resources/
  MyStageTest/    ← config files for MyStageTest (named after the test class)
    basic-test.conf
    edge-case.conf
  MyConnectorTest/
    test-config.conf
```

The resource subdirectory must be named after the test class. Load configs in tests with:

```java
Config config = ConfigFactory.load("MyStageTest/basic-test.conf");
```

## Test Configuration Files

Keep test configurations under `src/test/resources/`. By convention, each test class has its own subdirectory:

```
src/test/resources/
  MyStageTest/
    basic-test.conf
    edge-case.conf
  MyConnectorTest/
    test-config.conf
```

Load them in tests:

```java
Config config = ConfigFactory.load("MyStageTest/basic-test.conf");
```

Or build a config inline:

```java
Config config = ConfigFactory.parseString(
    "connectors: [{name: c1, class: \"...\", pipeline: p1, numDocs: 10}]\n" +
    "pipelines: [{name: p1, stages: []}]\n" +
    "indexer { type: Nop }"
);
RunResult result = Runner.runInTestMode(config);
```

## TestMessenger API

The `TestMessenger` captures all inter-component messages. It is accessible via `RunResult`:

| Method | Returns | Description |
|---|---|---|
| `result.getDocsSentForProcessing(connectorName)` | `List<Document>` | Documents the Connector published (sent to Workers). |
| `result.getDocsSentForIndexing(connectorName)` | `List<Document>` | Documents that completed the pipeline (sent to Indexer). |
| `result.getEvents(connectorName)` | `List<Event>` | All lifecycle events (CREATE, FINISH, FAIL, DROP). |
| `result.isSuccess()` | `boolean` | Whether the run completed without connector-level failure. |
| `result.getNumSucceeded(connectorName)` | `int` | Count of successfully indexed documents. |
| `result.getNumFailed(connectorName)` | `int` | Count of failed documents. |
| `result.getNumDropped(connectorName)` | `int` | Count of dropped documents. |

## JaCoCo Coverage Reports

After running tests with `mvn clean install`, open the coverage report:

```
lucille-core/target/jacoco-ut/index.html
```

This summarizes test coverage across packages and classes, showing covered and missed lines and branches.

## Testing Guidelines

- **One test class per component:** `MyStageTest` for `MyStage`, `MyConnectorTest` for `MyConnector`, etc. Group related assertions into focused test methods with descriptive names.
- **Maximize coverage:** Aim to cover as many branches, error paths, and edge cases as practical.
- **No network or external services:** Use `NopIndexer` or `RunType.TEST` to avoid real backends. Use mock objects for external APIs (HTTP, Kafka) only when necessary.
- **Exercise every parameter:** Each required and optional parameter should have at least one test path.
- **Test failures:** Verify bad configs throw the expected exceptions. Verify that documents with bad data fail gracefully without stopping the run.
- **Assert behavior:** Prefer testing state and interactions over log output.
- **Avoid sleeps:** Time-based assertions are fragile. Test mode is synchronous — the run completes before `runInTestMode()` returns.
- **Configuration clarity:** Use inline config strings in tests to make the configuration explicit and readable. Name each test config descriptively.

## Example: Full Pipeline Test

```java
@Test
public void testCsvThroughPipeline() throws Exception {
    Config config = ConfigFactory.parseString(
        "connectors: [{" +
        "  name: csv-conn, class: \"com.kmwllc.lucille.connector.FileConnector\"," +
        "  pipeline: p1, paths: [\"src/test/resources/test.csv\"]," +
        "  fileHandlers: { csv { idField: row_id } }" +
        "}]\n" +
        "pipelines: [{name: p1, stages: [" +
        "  {class: \"com.kmwllc.lucille.stage.TrimWhitespace\", fields: [\"title\"]}" +
        "]}]\n" +
        "indexer { type: Nop }"
    );

    RunResult result = Runner.runInTestMode(config);

    assertTrue(result.isSuccess());
    List<Document> docs = result.getDocsSentForIndexing("csv-conn");
    assertEquals(50, docs.size());  // 50 rows in test.csv

    // All titles should be trimmed
    for (Document doc : docs) {
        String title = doc.getString("title");
        assertEquals(title, title.trim());
    }
}
```
