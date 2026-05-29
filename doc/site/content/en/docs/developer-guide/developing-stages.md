---
title: Developing Stages
weight: 11
date: 2024-10-28
description: >
  How to implement a custom Stage for Lucille — skeleton, lifecycle, conditional execution, and the Document API.
---

To create a Stage, extend the abstract `Stage` class and implement `processDocument()`. That is the only method you are required to provide. The base class handles everything else: config validation, condition evaluation, metrics, thread isolation, and error routing.

**What the base class does for you:**

- **Config validation** — The constructor calls `getSpec().validate(config)` using your class's `SPEC` field. If the config has missing required properties, unknown properties, or type mismatches, validation fails at startup with a clear error message. You never call this yourself.
- **Condition evaluation** — If the user configures `conditions` on your stage, the base class evaluates them before calling `processDocument()`. If conditions are not met, your method is never invoked for that document. You write `processDocument()` as if conditions are always satisfied.
- **Thread isolation** — Each worker thread gets its own instance of your stage. Instance fields are effectively thread-local; no synchronization is needed.
- **Metrics** — The base class tracks per-stage document count, latency, error count, and child count automatically.
- **Error handling** — If `processDocument()` throws a `StageException`, the framework catches it, marks the document as failed, and continues processing other documents.

**What you implement:**

| Method | Required | Purpose |
|---|---|---|
| `processDocument(Document doc)` | Yes | Transform the document. Return `null` if no child documents are emitted, or an `Iterator<Document>` of children. |
| `start()` | No | Acquire resources (connections, models, compiled expressions) before processing begins. Called once per worker thread. |
| `stop()` | No | Release resources after processing ends. Called once per worker thread. |

**What you declare:**

| Field | Required | Purpose |
|---|---|---|
| `public static final Spec SPEC` | Yes | Declares the legal config properties for your stage. Accessed reflectively by the base class constructor. |

Your constructor must call `super(config)` — this triggers SPEC validation and condition parsing. After `super()` returns, you can safely read config values into instance fields.

**What's in `config`:** The `Config` passed to your Stage constructor contains only the properties defined inside your stage's config block — the `{ ... }` element from the `stages` list. It does not contain the full Lucille config. You read your parameters directly: `config.getString("myParam")`. Your SPEC should declare only the properties that belong to your stage.

---

## Stage Skeleton

Every stage must follow the [Javadoc Standards]({{< relref "docs/developer-guide/javadocs" >}}).

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

## Conditional Execution

Conditions are configured by the user in the stage's config block. Here's what that looks like:

```hocon
{
  class: "com.kmwllc.lucille.stage.MyStage"
  conditions: [
    { fields: ["status"], values: ["active"], operator: "must" }
  ]
}
```

**Guideline:** Prefer conditions over in-code skip logic. If your stage should only run on documents that have a certain field or a certain value, that decision belongs in the `conditions` config — not in `processDocument()`. This keeps stages reusable (the same stage can be applied with different conditions in different pipelines) and keeps the skip logic visible in the config rather than hidden in code.

There are legitimate exceptions. Some stages return early from `processDocument()` when a required source field is absent or empty — for example, `FetchUri` returns immediately if the URL field is missing or blank, `DetectLanguage` returns early if the accumulated text is shorter than a minimum length, and `ParseJson` returns early if its source field doesn't exist. These are cases where the stage's logic fundamentally cannot proceed and the check is more nuanced than a simple field-existence condition (e.g., checking `isEmpty()`, or evaluating a computed threshold). When you do return early, do so silently — don't throw a `StageException` for ordinary data variation.

For the full conditions reference and all other control flow options — skipping, dropping, error handling, child documents, and connector sequencing — see [Control Flow]({{< relref "docs/reference/control-flow" >}}).

## Stage Scope: One Stage or Several?

When designing a stage, you'll sometimes face the question of whether to build one stage that performs multiple internal steps, or several smaller stages that the user composes in config.

**Prefer a single stage when:**
- The intermediate state is only needed to connect internal operations and is not used by any other stage in the pipeline. Creating a field, using it once, and deleting it is three stages of ceremony for what should be one stage of work.
- Splitting would require the user to configure matching field names across stages and then clean up afterward.
- The combined operation is conceptually one thing from the user's perspective (e.g., "look up the hash of this field in a dictionary" — the hash is an implementation detail, not a user-visible artifact).

**Prefer separate stages when:**
- The intermediate result is actually used elsewhere in the pipeline — another stage reads it, it's indexed, or it's used in a condition.
- The operations are independently reusable *and* the user is actually reusing them independently in this pipeline.
- The operations need different conditions or different error handling.

**Pipeline simplicity is a valid design goal.** A pipeline where many stages exist only to create or clean up intermediate fields is harder to read, harder to maintain, and more error-prone than one where each stage does a complete unit of work. The user shouldn't have to think about plumbing between stages when the plumbing serves no purpose beyond connecting internals.

That said, if a stage becomes so large that it's doing several unrelated things and has a dozen config parameters, it's probably too big — not because of the intermediate field question, but because it's no longer a focused, testable unit.

**The balance:** One stage should do one *user-visible* thing, even if that thing involves multiple internal steps. The litmus test is: would a user in this pipeline benefit from the intermediate field existing as a separate, visible document field? If not, keep it internal to a single stage.

## Reading & Writing Fields

For the Document API — reading fields, writing fields, update modes, nested JSON, and supported types — see [The Document API]({{< relref "docs/developer-guide/quick-reference#the-document-api" >}}) in the Quick Reference.

## Fetching File Content

If your stage needs to read file content from a path stored on a document (e.g., fetching a PDF for text extraction, loading a dictionary file, reading a template), use `FileContentFetcher` rather than opening files directly. This gives your stage transparent support for local files, classpath resources, and cloud storage (S3, Azure, GCS) — and allows users to plug in a custom fetcher via the `fetcherClass` config property.

```java
private FileContentFetcher fileFetcher;

@Override
public void start() throws StageException {
  this.fileFetcher = FileContentFetcher.create(config);
  try {
    fileFetcher.startup();
  } catch (IOException e) {
    throw new StageException("Failed to initialize file fetcher", e);
  }
}

@Override
public Iterator<Document> processDocument(Document doc) throws StageException {
  String path = doc.getString("file_path");
  try (InputStream is = fileFetcher.getInputStream(path)) {
    // process the file content
  } catch (IOException e) {
    throw new StageException("Failed to fetch " + path, e);
  }
  return null;
}

@Override
public void stop() throws StageException {
  fileFetcher.shutdown();
}
```

By using `FileContentFetcher.create(config)`, your stage automatically supports the `fetcherClass` config property — users can substitute a custom implementation that resolves paths differently (e.g., fetching from a CMS API or interpreting paths relative to document metadata). See [Custom File Content Fetchers]({{< relref "docs/developer-guide/developing-file-handlers#custom-file-content-fetchers" >}}) for details on implementing a custom fetcher.

## Unit Testing

Stage unit tests follow a consistent pattern: create a stage from a config, create a document, call `processDocument()`, and assert on the document's state afterward.

### StageFactory

`StageFactory` eliminates the boilerplate of instantiating and starting a stage in tests. It handles reflection, config loading, and calling `start()` — returning a stage that's ready to process documents.

```java
private final StageFactory factory = StageFactory.of(MyStage.class);
```

`StageFactory` provides several `get()` overloads:

| Method | Use case |
|---|---|
| `factory.get("MyStageTest/config.conf")` | Load config from a test resource file |
| `factory.get(Map.of("source", "title", "dest", "out"))` | Build config inline from a map |
| `factory.get(config)` | Pass a pre-built `Config` object |
| `factory.get()` | Empty config (for stages with no required parameters) |

Each `get()` call creates a new stage instance and calls `start()` on it before returning.

### The basic pattern

```java
public class MyStageTest {

  private final StageFactory factory = StageFactory.of(MyStage.class);

  @Test
  public void testBasicBehavior() throws StageException {
    Stage stage = factory.get("MyStageTest/basic.conf");

    Document doc = Document.create("doc1");
    doc.setField("input", "hello");

    stage.processDocument(doc);

    assertEquals("HELLO", doc.getString("output"));
  }
}
```

### Testing conditions

Use `processConditional()` instead of `processDocument()` when you want to verify that conditions are evaluated correctly. `processDocument()` bypasses conditions; `processConditional()` respects them.

```java
@Test
public void testConditionalExecution() throws StageException {
  Stage stage = factory.get("MyStageTest/conditional.conf");

  Document matching = Document.create("doc1");
  matching.setField("status", "active");
  stage.processConditional(matching);
  assertTrue(matching.has("enriched"));  // stage ran

  Document nonMatching = Document.create("doc2");
  nonMatching.setField("status", "archived");
  stage.processConditional(nonMatching);
  assertFalse(nonMatching.has("enriched"));  // stage skipped
}
```

### Testing invalid configs

Use `assertThrows` to verify that bad configurations fail at startup (during construction or `start()`), not silently at runtime:

```java
@Test
public void testBadConfig() {
  assertThrows(StageException.class, () -> factory.get("MyStageTest/missingRequired.conf"));
  assertThrows(StageException.class, () -> factory.get("MyStageTest/invalidValue.conf"));
}
```

### Test config files

Store configs under `src/test/resources/` in a subdirectory named after the test class:

```
src/test/resources/
  MyStageTest/
    basic.conf
    conditional.conf
    missingRequired.conf
```

Each config file contains just the stage's config block — no pipelines, connectors, or indexer:

```hocon
{
  class = "com.kmwllc.lucille.stage.MyStage"
  source = "input"
  dest = "output"
}
```

### What to test

- **Each parameter** — at least one test path per required and optional parameter.
- **Conditions** — verify the stage is skipped when conditions are not met.
- **Invalid configs** — missing required fields, invalid values, type mismatches.
- **Edge cases** — empty fields, missing fields, multi-valued fields, null values.
- **Child documents** — if your stage emits children, assert on the returned iterator.

For full pipeline integration tests (running connectors, workers, and indexers end-to-end in memory), see [Testing Pipelines]({{< relref "docs/developer-guide/testing" >}}).
