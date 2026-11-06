---
weight: 50
title: Testing Pipelines
date: 2025-06-09
description: How to try out pipelines and write integration tests with Lucille's test mode (RunType.TEST and TestMessenger).
---

Lucille's test mode runs a complete pipeline end to end, against real source data, without sending anything to a search backend. Every document and event that passes between components is recorded in memory, so you can inspect it or assert on it after the run.

## Test mode is for pipeline development, not just tests

Despite its name, test mode is a supported part of `lucille-core`, not test-only code. You don't need an extra test dependency to use it, so it's available in any project that depends on Lucille. Use it to:

- **Develop a pipeline:** run it on sample data and look at the documents it would index, before any search backend is involved.
- **Check a change:** compare what a pipeline produces before and after you edit a stage or its config.
- **Test a pipeline:** assert on its output in a unit or integration test.

When the output looks right, run the same config for real. Neither the config nor your code needs to change; only the call does:

```java
// dry run: nothing is sent to the search backend; documents and events are recorded
Map<String, TestMessenger> history = Runner.runInTestMode(config);
history.get("my-connector").getDocsSentForIndexing().forEach(System.out::println);

// real run: the same config, indexing for real
RunResult result = Runner.run(config, Runner.RunType.LOCAL);
```

Keep in mind:

- **Only indexing is skipped.** Connectors and stages still read from and call the systems they're configured for: files, databases, S3, HTTP APIs, embedding services. Point the config at sample or test sources if that matters.
- **Everything is kept in memory** until the run returns, so use test mode on a sample of your data, not a full load.

## RunType.TEST

When a run is started with `RunType.TEST`, Lucille:

- Runs all components (Connector, Workers, Indexer) as in a local run.
- **Skips sending to the search backend.** The Indexer runs its full code path (batching, events, error handling) up to the point where it would send, so no actual indexing occurs.
- **Records all messages** flowing between components in a `TestMessenger`, one per connector.

## Running in Test Mode

`Runner.runInTestMode(config)` returns the recordings, keyed by connector name. If you also need the run's status, call `Runner.run(config, Runner.RunType.TEST)` instead, which returns a `RunResult`; its `getHistory()` holds the same recordings.

```java
import com.kmwllc.lucille.core.Runner;
import com.kmwllc.lucille.core.RunResult;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@Test
public void testMyPipeline() throws Exception {
    Config config = ConfigFactory.load("my-pipeline-test.conf");
    RunResult result = Runner.run(config, Runner.RunType.TEST);

    assertTrue("Run should succeed", result.getStatus());
    TestMessenger history = result.getHistory().get("my-connector");
}
```

## Asserting on Documents

After the run, you can inspect what documents were sent for processing and what reached the (bypassed) Indexer:

```java
TestMessenger history = Runner.runInTestMode(config).get("my-connector");

// Documents the connector published (sent for processing by Workers)
List<Document> published = history.getDocsSentForProcessing();

// Documents that completed the pipeline (sent for indexing)
List<Document> indexed = history.getDocsSentForIndexing();

assertEquals(100, published.size());
assertEquals(95, indexed.size());  // 5 were dropped

// Inspect individual documents
Document first = indexed.get(0);
assertEquals("expected-value", first.getString("my_field"));
assertTrue(first.has("enriched_field"));
```

## Asserting on Events

```java
List<Event> events = history.getSentEvents();

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
    "indexer { class: \"com.kmwllc.lucille.indexer.NopIndexer\" }"
);
Map<String, TestMessenger> history = Runner.runInTestMode(config);
```

## TestMessenger API

`Runner.runInTestMode(config)` returns one `TestMessenger` per connector, keyed by connector name (`RunResult.getHistory()` returns the same map). These methods are its supported API:

| Method | Returns | Description |
|---|---|---|
| `getDocsSentForProcessing()` | `List<Document>` | Documents the Connector published (sent to Workers). |
| `getDocsSentForIndexing()` | `List<Document>` | Documents that completed the pipeline (sent to the Indexer). |
| `getSentEvents()` | `List<Event>` | All lifecycle events (CREATE, FINISH, FAIL, DROP), in the order they were sent. |

`TestMessenger` also implements Lucille's internal messenger interfaces; those methods are not part of its supported API and may change. For the run's overall outcome, use `RunResult.getStatus()` from `Runner.run(config, Runner.RunType.TEST)`.

## JaCoCo Coverage Reports

After running tests with `mvn clean install`, open the coverage report:

```
lucille-core/target/jacoco-ut/index.html
```

This summarizes test coverage across packages and classes, showing covered and missed lines and branches.

## Controlling Test Parallelism

By default the build runs test classes in parallel (one JVM fork per CPU core) and builds Lucille's modules in
parallel. Two independent settings change this per run:

- `-Dsurefire.forkCount=<N|NC>` — test forks within a module. Use `1` to run a module's tests serially in a single JVM
  (useful when debugging an interaction between tests, or to get un-interleaved console output); `2`, `0.5C`, etc. to
  tune.
- `-T<N|NC>` — Maven reactor threads: how many modules Maven builds at once. The reactor is the set of Lucille modules
  built together; the plugin and example modules all depend on `lucille-core`, so `lucille-core` builds first and the
  modules that depend on it build in parallel once it finishes.

Fully serial build (slowest, most deterministic output):

```bash
mvn clean install -T1 -Dsurefire.forkCount=1
```

### Identifying which fork produced output

When tests run in parallel forks, output from different test classes interleaves, so you need a way to tell which fork
a line came from. Surefire numbers each fork (`1`..`forkCount`), and the build passes that number to the forked JVM as
the `surefire.forkNumber` system property.

- **Console:** each log line starts with its fork number, e.g. `[fork 2] ...`, so you can follow a single fork's output
  through the interleaved stream (and `grep` for one fork).
- **Log files:** each fork writes its own files under `log/`, named with the fork number (e.g.
  `log/com.kmwllc.lucille-1.log`, `log/com.kmwllc.lucille-2.log`, `log/heartbeat-1.log`), rather than all forks sharing
  one file. Open the file matching the fork number from the console line for that test.

With `-Dsurefire.forkCount=1` there is a single fork, so output is not interleaved and the fork number is always `1`.
Tests run outside Maven, for example from an IDE, have no fork number and use `0`.

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
        "indexer { class: \"com.kmwllc.lucille.indexer.NopIndexer\" }"
    );

    RunResult result = Runner.run(config, Runner.RunType.TEST);

    assertTrue(result.getStatus());
    List<Document> docs = result.getHistory().get("csv-conn").getDocsSentForIndexing();
    assertEquals(50, docs.size());  // 50 rows in test.csv

    // All titles should be trimmed
    for (Document doc : docs) {
        String title = doc.getString("title");
        assertEquals(title, title.trim());
    }
}
```
