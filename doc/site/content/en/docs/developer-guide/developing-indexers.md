---
title: Developing Indexers
weight: 13
date: 2024-10-28
description: >
  How to implement a custom Indexer for Lucille — skeleton, lifecycle, and sending documents to a destination.
---

To create an Indexer, extend the abstract `Indexer` class and implement three methods: `sendToIndex()`, `checkConnection()`, and `closeConnection()`. The base class manages the entire consumption loop — polling documents from the indexing queue, accumulating them into batches, flushing batches on size or timeout, sending completion/failure events, retrying transient failures, and logging throughput metrics.

`Indexer` is an abstract class (like `Stage`) rather than an interface because the framework drives the indexer's execution. The base class implements `Runnable` and its `run()` method contains the message consumption loop, batching logic, retry machinery, and event accounting. Your implementation only provides the transport-specific operations: validate the connection, send a batch, and close the connection.

**What the base class does for you:**

- **Message consumption loop** — Polls the indexing queue, accumulates documents into batches, and flushes on batch size or timeout. You never write this loop.
- **Config validation** — Validates both the generic `indexer` config block (batchSize, field filtering, deletion markers, retry settings) and your implementation-specific block nested inside it (e.g., `indexer.solr`, `indexer.opensearch`) using your `SPEC`.
- **Field filtering** — Applies `includeFields`/`excludeFields` filtering before documents reach `sendToIndex()`. Use `getConvertedIndexerDoc(doc)` to get the filtered field map as plain Java values, or `getRawIndexerDoc(doc)` when your client serializes it to JSON with Jackson.
- **Batching** — Configurable batch size and timeout, with support for per-document index routing via `indexOverrideField`.
- **Concurrent sends** — When `indexer.maxConcurrentBatches` is above 1, sends several batches to the destination at once while keeping writes to the same document id in order and completing batches in dispatch order. See [Concurrent Sends](#concurrent-sends) below.
- **Retry with backoff** — When `indexer.maxRetries` is configured, retries failed batches with exponential backoff and jitter. Your `sendToIndex()` returns per-document failures; the base class decides whether to retry based on status codes. A retry sends only the documents that are still pending, not the whole batch. See [Retries and the `sendToIndex()` Contract](#retries-and-the-sendtoindex-contract).
- **Event accounting** — Sends FINISH or FAIL events for each document so the Runner can track run completion.
- **Metrics** — Tracks documents indexed, throughput rate, and batch latency automatically.
- **ID and index overrides** — Supports `idOverrideField` and `indexOverrideField` for routing documents to different IDs or indices. Use `getDestinationId(doc)` for the ID to write under, which is the `idOverrideField` value or else the document ID, and `getIndexOverride(doc)` for the target index, which is null when the document has no override.
- **Deletion support** — Detects documents marked for deletion via `deletionMarker`, and delete-by-query documents via `deleteByQuery`. Your implementation checks these in `sendToIndex()` and issues the appropriate delete operation.

**What stays with your indexer:** these depend on the destination, and the built-in indexers handle them differently.

- **A filtered `id` field** — `getConvertedIndexerDoc(doc)` and `getRawIndexerDoc(doc)` leave `id` out when the field filter removes it. If your destination carries the ID separately, as OpenSearch and Elasticsearch do with `_id`, there is nothing more to do. If it needs the ID in the body, as Solr does, add `getDestinationId(doc)` yourself.
- **The index override field** — it stays in the filtered fields. The Solr indexer removes it before sending. The OpenSearch indexer sends it, and users who don't want it indexed can blacklist it.
- **Children** — neither method includes them. Read them from `doc.getChildren()` and send them however your destination represents them.

**What you implement:**

| Method | Required | Purpose |
|---|---|---|
| `sendToIndex(PendingSend send)` | Yes | Send `send.documents()` to the destination. Return an `IndexerFailure(document, cause)` for each document that was not applied, holding the instance from `send.documents()`, and report each instance only once; a failure that breaks either rule fails the rest of the batch. Return an empty list if all succeeded. Not called in bypass mode. Throw `IndexerException` if nothing in the send was applied, or `IndexerRetryableException` if that failure may be transient, so the retry policy applies. See [Retries and the `sendToIndex()` Contract](#retries-and-the-sendtoindex-contract). |
| `checkConnection()` | Yes | Return `true` if the destination is reachable and the target index/collection exists. Called before the consumption loop starts, through the base class's `validateConnection()`. Not called in bypass mode. |
| `closeConnection()` | Yes | Close the client or connection to the destination. |
| `supportsConcurrentSends()` | No | Return `true` only if `sendToIndex()` and your destination client are safe to call from several threads at once. The default is `false`, which makes the base class reject `indexer.maxConcurrentBatches` above 1. See [Concurrent Sends](#concurrent-sends). |
| `getIndexerConfigKey()` | Yes | Return the key of your implementation-specific block inside the `indexer` block (e.g., `"solr"`, `"opensearch"`). Return `null` if your indexer takes no additional config. |
| `getUnsupportedSettings()` | No | Return the names of generic `indexer` settings your indexer can't apply, so that setting one fails at startup instead of being ignored. The default is none. |

**What you declare:**

| Field | Required | Purpose |
|---|---|---|
| `public static final Spec SPEC` | Yes | Declares the legal properties for your implementation-specific config block (not the generic `indexer` block — that's validated by the base class). |

Your constructor must call `super(config, context)` — this triggers config validation, sets up batching, and initializes metrics. The constructor signature must be `(Config, IndexerContext)` because the `IndexerFactory` instantiates indexers reflectively using this signature. `IndexerContext` holds what the framework provides apart from config, such as the messenger and the metrics prefix. Pass it through to `super`; the base class exposes what you need, such as `isBypass()`.

**What's in `config`:** Like Stages and Connectors, your Indexer receives its own block: the `Config` passed to your constructor is the `indexer` block. The base class reads generic settings from it, such as `idOverrideField`. Your implementation reads its nested block via `config.getConfig("mykey")` (e.g., `config.getConfig("solr")`). Your SPEC declares only the properties within your implementation-specific block — write `"url"`, not `"solr.url"`. The base class validates the generic settings separately.

**Two-level config:** The generic settings in `indexer {}` (batch size, field filtering, deletion markers, retries) are validated and consumed by the base class. Your implementation-specific block nested inside it (e.g., `solr {}`, `opensearch {}`) is validated against your `SPEC` and read in your constructor. Your SPEC should only declare properties within your block — write `"url"`, not `"solr.url"`.

---

## Indexer Skeleton

Every indexer must follow the [Javadoc Standards]({{< relref "docs/developer-guide/javadocs" >}}).

```java
package com.kmwllc.lucille.indexer;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.IndexerContext;
import com.kmwllc.lucille.core.IndexerFailure;
import com.kmwllc.lucille.core.PendingSend;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
import java.util.List;

/**
 * One-line summary of what this Indexer does and where it sends documents.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>url (String, Required) : Destination endpoint (e.g., base URL).</li>
 *   <li>index (String, Optional) : Default index/collection name. Defaults to "index1".</li>
 * </ul>
 */
public class ExampleIndexer extends Indexer {
  
  public static final Spec SPEC = SpecBuilder.indexer()
      .requiredString("url")
      .optionalString("index")
      .build();
  
  private final String url;
  private final String defaultIndex;
  
  public ExampleIndexer(Config config, IndexerContext context) {
    super(config, context);
    Config implConfig = config.getConfig("example");
    this.url = implConfig.getString("url");
    this.defaultIndex = ConfigUtils.getOrDefault(implConfig, "index", "index1");
  }

  @Override
  protected String getIndexerConfigKey() {
    return "example";  // matches the nested block name: indexer { example { url: "...", index: "..." } }
  }

  @Override
  protected boolean checkConnection() {
    // Health check to the destination — return false if unreachable
    return true;
  }

  @Override
  protected List<IndexerFailure> sendToIndex(PendingSend send) throws IndexerException {
    List<Document> documents = send.documents();
    // Send the documents using your destination client.
    // Wrap a client failure that stops the whole send in IndexerException, or in IndexerRetryableException
    // (with the HTTP status, if there is one) when a retry may succeed.
    // Use getConvertedIndexerDoc(doc) for the filtered field map as plain Java values,
    // or getRawIndexerDoc(doc) if your client serializes it to JSON with Jackson.
    // Use getDestinationId(doc) for the id to write under, so that idOverrideField is honored.
    // Return an IndexerFailure for each failed doc; empty list if all succeeded.
    return List.of();
  }

  @Override
  public void closeConnection() {
    // Close client resources
  }
}
```

## Retries and the `sendToIndex()` Contract

`sendToIndex()` takes one argument, a `PendingSend`, which describes the call the base class is about to make:

| Method | Returns |
|---|---|
| `documents()` | The documents to send now, in the order they were batched. On the first attempt this is the whole batch; on a retry it is only the documents still pending. The list is unmodifiable, and the documents are the same instances on every attempt. |
| `attempt()` | This attempt's number, starting at 1. |
| `isFinal()` | `true` when no retry can follow this attempt, whatever its outcome. Always `true` when `indexer.maxRetries` is not set. |
| `PendingSend.of(documents)` | A send made once, with no retry (`attempt()` is 1, `isFinal()` is `true`). Use it to call `sendToIndex()` directly in a test. |

Most indexers only read `send.documents()`. The base class creates a new `PendingSend` for each attempt, and more members may be added to it in later releases without changing your indexer.

**The contract.** `sendToIndex()` sends the documents in `send.documents()` and returns one `IndexerFailure` for each of them that was not applied.

1. A document that is not in the returned list counts as applied. That includes a document you collapsed into a later operation on the same id.
2. Each returned failure must hold a `Document` instance from `send.documents()`, and each instance may be reported only once. A failure that breaks either rule cannot be matched to a document, so no retry is made and every document in the batch without a failure of its own is failed.
3. Throw only when nothing in the call was applied, or when you cannot tell. If you applied part of the send before an error, return per-document failures instead. The base class cannot see inside a call that throws, so after a throw it treats every document in the send as still pending.
4. Use an `IndexerRetryableException` as the cause of a failure that may be transient, and an `IndexerException` for a permanent one.
5. If you stop part-way on purpose, because a later document must not be sent before an earlier one that failed, report each document you held back with `IndexerFailure.notSent(document)`.

**What the base class does with the result.** After each attempt, every document in the send is in one of three states:

| The attempt... | The document... |
|---|---|
| does not report it | was applied. It is never sent again. |
| reports it with a cause that is not retryable | has failed for good. It is never sent again. |
| reports it with a retryable cause, or as not sent | is still pending. |

A cause is retryable if it is an `IndexerRetryableException` whose status code is in `indexer.retryableStatusCodes`. If an attempt returned at least one retryable failure and attempts remain, the base class waits and calls `sendToIndex()` again with the pending documents, in their original order. Otherwise the batch is finished: applied documents get FINISH events, and every other document gets a FAIL event with its cause.

- **A not-sent document** never causes a retry by itself. If no further attempt is made, it fails, and its FAIL event names the first retryable failure of the last attempt.
- **A thrown retryable exception** leads to another attempt with the same documents.
- **A thrown exception that ends the attempts** (it is not retryable, or retries have run out) fails the documents that are still pending. Documents that an earlier attempt applied still get FINISH events. If no earlier attempt applied or permanently failed anything, the whole batch fails.
- **Without `indexer.maxRetries`** there is one call, with the whole batch.

**Order across attempts.** A retry can send a document after a later document from the same batch has been applied. If order matters to your destination (for example, a delete-by-query must not run before an earlier upload that it should remove), stop at the first retryable failure and return the rest as `notSent`, unless `send.isFinal()` is true. On the final attempt nothing follows, so send everything that is left.

**In-doubt requests.** If the destination applied a write but the response was lost (for example, a timeout), the write is not known to have been applied, and it is sent again.

## Packaging an Indexer in a Plugin Module

Indexers that pull in a heavy or backend-specific dependency (a search client, a vector-store SDK, native libraries) do **not** live in `lucille-core`. 
They live in their own plugin module under `lucille-plugins/`, so that `lucille-core` stays lightweight and users only pull in the client stacks they actually use. 
The Solr, OpenSearch, and Elasticsearch indexers are packaged this way. `CSVIndexer` and `NopIndexer` are still built-in.

If your indexer needs no third-party dependency, put it in `lucille-core` and you are done, skip this section. 
Otherwise, follow the checklist below to add a new plugin module. The same module mechanics apply to a plugin stage or connector. Only the `indexer.type` registration step is indexer-specific.

### 1. Create the module

Create `lucille-plugins/lucille-<name>/` with a `pom.xml` modeled on an existing backend plugin (for example `lucille-solr`). Key points:

- Parent is `lucille-plugins`.
- Depend on `lucille-core` with `<scope>provided</scope>`
- Declare your backend client dependency.
- Include the `maven-shade-plugin` block bound to `${shaded-plugin-jar.phase}` with the `ServicesResourceTransformer` (copy it verbatim from `lucille-solr`); this keeps the shaded `*-plugin.jar` option working and merges service files correctly.

Register the module in `lucille-plugins/pom.xml`.

### 2. Put the indexer in a backend-specific package

Use `com.kmwllc.lucille.<name>.indexer.<Name>Indexer` (e.g. `com.kmwllc.lucille.solr.indexer.SolrIndexer`), not `com.kmwllc.lucille.indexer`. Shared HTTP helpers like `SSLUtils`, `HttpHostUtils`, `HttpClientConfigUtils`, and `AsyncConnectionPoolUtils` stay in `lucille-core`'s `util` package — import them from there.

### 3. Register the indexer's type (optional)

Registering lets the nested block's name, or `indexer.type: "..."`, select your indexer. The type name must match your block's name, ignoring case, for the block to select the indexer. Types are resolved at runtime from a service file on the classpath, so a plugin can register its type name without `lucille-core` referencing the class. You never hand-write that file: [Google AutoService](https://github.com/google/auto/tree/main/service) generates it at compile time from annotations. AutoService is already wired into the build (`auto-service-annotations` on the compile classpath and the `auto-service` processor in the compiler plugin's `annotationProcessorPaths`, both inherited from `lucille-parent`), so the only thing you write is the annotation:

```java
import com.google.auto.service.AutoService;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.IndexerContext;
import com.kmwllc.lucille.indexer.IndexerType;

@AutoService(Indexer.class)
@IndexerType("<name>")
public class NameIndexer extends Indexer {
  ...
}
```

`@IndexerType` declares the `indexer.type` name; `@AutoService(Indexer.class)` makes AutoService list this class in `META-INF/services/com.kmwllc.lucille.core.Indexer` at compile time. At runtime `IndexerFactory` reads the class names from those service files (merged across all jars on the classpath), resolves each with `Class.forName(name, false, loader)` which loads the class and reads its `@IndexerType` **without instantiating it**, and matches the annotation value case-insensitively against the nested block's name or the configured `indexer.type`. Only the matching indexer is constructed, through its normal `(Config, IndexerContext)` constructor.

If you skip this step, your indexer is still usable, but users must set the fully-qualified `indexer.class`.

> **Note:** AutoService runs as an annotation processor during compilation. If you add or change an `@IndexerType` and the generated service file seems stale (the type won't resolve), do a clean build (`mvn clean ...`). Incremental compilation can skip the processor when it considers the class files up to date.

#### External projects

A third-party project that uses Lucille as a dependency registers a custom indexer the same way. It already depends on `lucille-core` (which provides `@IndexerType` and the `Indexer` base class). It additionally needs AutoService on its own build's annotation-processor path — either the `auto-service` processor in its `maven-compiler-plugin` `annotationProcessorPaths`, or the equivalent for its build tool — plus `auto-service-annotations` on the compile classpath for the `@AutoService` annotation. With those in place, annotating the class makes its jar carry its own generated service file, and dropping that jar on the classpath makes `indexer.type` resolve the same way, with no change to Lucille.

### 4. Add the module to the BOM

Add your module to `lucille-bom/pom.xml` so that examples and downstream projects can depend on it without specifying a version.

### 5. Add it to `lucille-all`

Add your module as a dependency in `lucille-all/pom.xml`. `lucille-all` bundles every component plugin into one classpath for the batteries-included Docker image, and its `PackagingTest` verifies that all `indexer.type` names still resolve once everything is on the classpath together — which catches `META-INF/services` merge problems and cross-plugin dependency conflicts. Add your expected type to that test.

### 6. Document it

Add a page under `doc/site/content/en/docs/ingest-design/indexers/` following the convention of the existing backend pages: the fully-qualified class name, a `*(requires lucille-<name>)*` note, and a **Maven dependency** block. List it in the indexers `_index.md` catalogue.

---

## Concurrent Sends

`indexer.maxConcurrentBatches` (default 1) lets the base class keep several bulk requests in flight at once. At 1, each batch is sent on the indexer thread and completed before the next. Above 1, the base class calls your `sendToIndex()` from a pool of threads. It guarantees that no two batches in flight write the same destination id, that a batch containing a delete-by-query runs alone, and that batches are completed in the order they were dispatched. Your `sendToIndex()` still handles one batch and does not change.

What you must provide is thread safety, and you opt in by overriding `supportsConcurrentSends()`:

```java
@Override
protected boolean supportsConcurrentSends() {
  // Only if the destination client may be called from several threads at once.
  return true;
}
```

- **The destination client must be safe for concurrent use.** Create it once in the constructor, as usual, but do not rely on per-call state inside it. If the client's thread safety is unknown, leave the default (`false`).
- **Do not keep per-batch state on the indexer instance** (buffers, counters, "current batch" fields). Two calls to `sendToIndex()` can overlap.
- **The base class calls `supportsConcurrentSends()` from its constructor**, so it must return a constant and not depend on your subclass fields.
- **Use `isMarkedForDeletion(doc)` and `isDeleteByQuery(doc)`** to detect deletes, and `getDeleteByQueryFieldName(doc)` and `getDeleteByQueryValue(doc)` for a delete-by-query's target. The dispatcher uses the same checks to decide which batches are delete-by-query barriers.
- **Connection pools.** If your client is HTTP-based, size its pool for at least `maxConcurrentBatches` simultaneous requests, or the sends will queue for a connection.

To test, set `indexer.maxConcurrentBatches` in the config you pass to your indexer and assert that your `sendToIndex()` can run twice at once without errors. See [Indexing Throughput]({{< relref "docs/operations/indexing-throughput" >}}) for the guarantees, tuning, and Kafka requirements.

## Unit Testing

Indexer tests verify that `sendToIndex()` sends the correct data to the destination. The standard pattern is: create a `TestMessenger`, place documents on it for indexing, instantiate your indexer with a mock client, run it for a fixed number of iterations, and assert on what was sent to the mock.

### The basic pattern

```java
public class MyIndexerTest {

  @Test
  public void testBasicIndexing() throws Exception {
    Config config = ConfigFactory.empty()
        .withValue("batchSize", ConfigValueFactory.fromAnyRef(1));
    TestMessenger messenger = new TestMessenger();

    Document doc = Document.create("doc1", "test_run");
    doc.setField("title", "Hello");

    MyClient mockClient = mock(MyClient.class);
    Indexer indexer = new MyIndexer(config, new IndexerContext(messenger, false, "", null), mockClient);

    messenger.sendForIndexing(doc);
    indexer.run(1);

    verify(mockClient, times(1)).send(any());
  }
}
```

### run(iterations) — the fixed-iteration polling loop

The `Indexer` base class provides `run(int iterations)` alongside the standard `run()`. While `run()` polls indefinitely until `terminate()` is called (designed for production use), `run(iterations)` polls exactly N times and then flushes the final batch. This is the key testing mechanism — it lets you control exactly how many poll cycles the indexer executes without needing threads or timeouts.

Each iteration polls one document from the messenger's indexing queue. If you place 3 documents on the messenger and call `indexer.run(3)`, the indexer will poll all 3, batch them according to `batchSize`, send them to your `sendToIndex()`, and then flush any remaining partial batch before closing.

### TestMessenger as the indexing queue

`TestMessenger` simulates the messaging layer. Use `messenger.sendForIndexing(doc)` to place documents on the indexing queue before calling `run()`. After the run, use `messenger.getSentEvents()` to verify that the indexer sent the expected FINISH or FAIL events.

```java
messenger.sendForIndexing(doc1);
messenger.sendForIndexing(doc2);
indexer.run(2);

List<Event> events = messenger.getSentEvents();
assertEquals(2, events.size());
assertEquals(Event.Type.FINISH, events.get(0).getType());
assertEquals(Event.Type.FINISH, events.get(1).getType());
```

### Mocking the destination client

Indexers that talk to external systems (Solr, OpenSearch, Elasticsearch) should inject a mock client. The common pattern is a constructor overload:

```java
// Production constructor — creates a real client
public MyIndexer(Config config, IndexerContext context) {
  this(config, context, createRealClient(config));
}

// Test constructor — accepts a mock
public MyIndexer(Config config, IndexerContext context, MyClient client) {
  super(config, context);
  this.client = client;
}
```

Use Mockito's `ArgumentCaptor` to inspect what was sent to the mock:

```java
ArgumentCaptor<List<MyDocument>> captor = ArgumentCaptor.forClass(List.class);
verify(mockClient, times(1)).bulkIndex(captor.capture());
assertEquals(2, captor.getValue().size());
assertEquals("doc1", captor.getValue().get(0).getId());
```

### What to test

- **Successful indexing** — documents reach the destination with correct fields and IDs.
- **Field translation** — your `sendToIndex()` correctly maps Document fields to the destination's format.
- **ID override** — documents are written under `getDestinationId(doc)` when `idOverrideField` is configured.
- **Deletion** — documents marked for deletion trigger delete operations instead of adds.
- **Per-document failures** — your `sendToIndex()` returns the correct `IndexerFailure`s when individual documents fail. To call it directly, pass `PendingSend.of(documents)`.
- **checkConnection()** — returns false when the destination is unreachable.
- **closeConnection()** — releases client resources without throwing.

For full pipeline integration tests (running connectors through pipelines and indexers end-to-end in memory), see [Testing Pipelines]({{< relref "docs/developer-guide/testing" >}}).
