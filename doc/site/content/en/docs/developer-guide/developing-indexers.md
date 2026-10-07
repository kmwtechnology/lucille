---
title: Developing Indexers
weight: 13
date: 2024-10-28
description: >
  How to implement a custom Indexer for Lucille — skeleton, lifecycle, and sending documents to a destination.
---

To create an Indexer, extend the abstract `Indexer` class and implement three methods: `sendToIndex()`, `validateConnection()`, and `closeConnection()`. The base class manages the entire consumption loop — polling documents from the indexing queue, accumulating them into batches, flushing batches on size or timeout, sending completion/failure events, retrying transient failures, and logging throughput metrics.

`Indexer` is an abstract class (like `Stage`) rather than an interface because the framework drives the indexer's execution. The base class implements `Runnable` and its `run()` method contains the message consumption loop, batching logic, retry machinery, and event accounting. Your implementation only provides the transport-specific operations: validate the connection, send a batch, and close the connection.

**What the base class does for you:**

- **Message consumption loop** — Polls the indexing queue, accumulates documents into batches, and flushes on batch size or timeout. You never write this loop.
- **Config validation** — Validates both the generic `indexer` config block (batchSize, field filtering, deletion markers, retry settings) and your implementation-specific config block (e.g., `solr`, `opensearch`) using your `SPEC`.
- **Field filtering** — Applies whitelist/blacklist filtering before documents reach `sendToIndex()`. Use `getIndexerDoc(doc)` to get the filtered field map.
- **Batching** — Configurable batch size and timeout, with support for per-document index routing via `indexOverrideField`.
- **Retry with backoff** — When `indexer.maxRetries` is configured, retries failed batches with exponential backoff and jitter. Your `sendToIndex()` returns per-document failures; the base class decides whether to retry based on status codes.
- **Event accounting** — Sends FINISH or FAIL events for each document so the Runner can track run completion.
- **Metrics** — Tracks documents indexed, throughput rate, and batch latency automatically.
- **ID and index overrides** — Supports `idOverrideField` and `indexOverrideField` for routing documents to different IDs or indices. Use `getDocIdOverride(doc)` and `getIndexOverride(doc)` in your `sendToIndex()`.
- **Deletion support** — Detects documents marked for deletion via `deletionMarkerField`/`deletionMarkerFieldValue` or `deleteByFieldField`/`deleteByFieldValue`. Your implementation checks these in `sendToIndex()` and issues the appropriate delete operation.

**What you implement:**

| Method | Required | Purpose |
|---|---|---|
| `sendToIndex(List<Document> docs)` | Yes | Send a batch to the destination. Return a set of `Pair<Document, Exception>` for any per-document failures; return an empty set if all succeeded. |
| `validateConnection()` | Yes | Return `true` if the destination is reachable and the target index/collection exists. Called before the consumption loop starts. |
| `closeConnection()` | Yes | Close the client or connection to the destination. |
| `getIndexerConfigKey()` | Yes | Return the config key for your implementation-specific block (e.g., `"solr"`, `"opensearch"`). Return `null` if your indexer takes no additional config. |

**What you declare:**

| Field | Required | Purpose |
|---|---|---|
| `public static final Spec SPEC` | Yes | Declares the legal properties for your implementation-specific config block (not the generic `indexer` block — that's validated by the base class). |

Your constructor must call `super(config, messenger, bypass, metricsPrefix, localRunId)` — this triggers config validation, sets up batching, and initializes metrics. The constructor signature must be `(Config, IndexerMessenger, boolean, String, String)` because the `IndexerFactory` instantiates indexers reflectively using this signature.

**What's in `config`:** Unlike Stages and Connectors, the `Config` passed to your Indexer constructor is the **full root config** for the entire Lucille run. The base class reads generic settings from `config.getString("indexer.idOverrideField")` etc. Your implementation reads its own block via `config.getConfig("mykey")` (e.g., `config.getConfig("solr")`). Your SPEC declares only the properties within your implementation-specific block — write `"url"`, not `"solr.url"`. The base class validates the generic `indexer` block separately.

**Two-level config:** Unlike Stages and Connectors, Indexers have a split config. The generic `indexer {}` block (batch size, field filtering, deletion markers, retries) is validated and consumed by the base class. Your implementation-specific block (e.g., `solr {}`, `opensearch {}`) is validated against your `SPEC` and read in your constructor. Your SPEC should only declare properties within your block — write `"url"`, not `"solr.url"`.

---

## Indexer Skeleton

Every indexer must follow the [Javadoc Standards]({{< relref "docs/developer-guide/javadocs" >}}).

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
  
  public ExampleIndexer(Config config, IndexerMessenger messenger, boolean bypass,
      String metricsPrefix, String localRunId) {
    super(config, messenger, bypass, metricsPrefix, localRunId);
    Config implConfig = config.getConfig("example");
    this.url = implConfig.getString("url");
    this.defaultIndex = ConfigUtils.getOrDefault(implConfig, "index", "index1");
  }

  @Override
  protected String getIndexerConfigKey() {
    return "example";  // matches the config block name: example { url: "...", index: "..." }
  }

  @Override
  public boolean validateConnection() {
    // Health check to the destination — return false if unreachable
    return true;
  }

  @Override
  protected Set<Pair<Document, Exception>> sendToIndex(List<Document> documents) throws Exception {
    // Send the batch using your destination client.
    // Use getIndexerDoc(doc) to get the filtered field map.
    // Use getDocIdOverride(doc) if idOverrideField may be configured.
    // Return any failed docs as Pair<Document, Exception>; empty set if all succeeded.
    return Set.of();
  }

  @Override
  public void closeConnection() {
    // Close client resources
  }
}
```

## Unit Testing

Indexer tests verify that `sendToIndex()` sends the correct data to the destination. The standard pattern is: create a `TestMessenger`, place documents on it for indexing, instantiate your indexer with a mock client, run it for a fixed number of iterations, and assert on what was sent to the mock.

### The basic pattern

```java
public class MyIndexerTest {

  @Test
  public void testBasicIndexing() throws Exception {
    Config config = ConfigFactory.empty()
        .withValue("indexer.batchSize", ConfigValueFactory.fromAnyRef(1));
    TestMessenger messenger = new TestMessenger();

    Document doc = Document.create("doc1", "test_run");
    doc.setField("title", "Hello");

    MyClient mockClient = mock(MyClient.class);
    Indexer indexer = new MyIndexer(config, messenger, false, "", null, mockClient);

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
public MyIndexer(Config config, IndexerMessenger messenger, boolean bypass,
    String metricsPrefix, String localRunId) {
  this(config, messenger, bypass, metricsPrefix, localRunId, createRealClient(config));
}

// Test constructor — accepts a mock
public MyIndexer(Config config, IndexerMessenger messenger, boolean bypass,
    String metricsPrefix, String localRunId, MyClient client) {
  super(config, messenger, bypass, metricsPrefix, localRunId);
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
- **ID override** — `getDocIdOverride(doc)` is used when `idOverrideField` is configured.
- **Deletion** — documents marked for deletion trigger delete operations instead of adds.
- **Per-document failures** — your `sendToIndex()` returns the correct `Pair<Document, Exception>` set when individual documents fail.
- **validateConnection()** — returns false when the destination is unreachable.
- **closeConnection()** — releases client resources without throwing.

For full pipeline integration tests (running connectors through pipelines and indexers end-to-end in memory), see [Testing Pipelines]({{< relref "docs/developer-guide/testing" >}}).
