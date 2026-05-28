---
title: Developing Connectors
weight: 12
date: 2024-10-28
description: >
  How to implement a custom Connector for Lucille — skeleton, lifecycle, and publishing documents.
---

To create a Connector, extend `AbstractConnector` and implement `execute()`. That is the only method you are required to provide. The `execute()` method receives a `Publisher` — your connector reads from its data source and calls `publisher.publish(doc)` for each document it produces. That's the core job of a connector: create documents and publish them. The base class handles config validation, name/pipeline resolution, document ID prefixing, and provides no-op defaults for the optional lifecycle methods.

**Why Connector is an interface:** Unlike `Stage` and `Indexer`, which are abstract classes, `Connector` is defined as an interface. This is because connectors have no processing loop managed by the framework — a connector owns its own execution entirely. The Runner calls `execute()` and the connector decides how to read data and when to publish documents. There is no framework-managed thread pool, no batching, and no message consumption loop to build into a base class. `AbstractConnector` provides the common config parsing and SPEC validation, but the execution model is entirely yours.

By contrast, `Stage` and `Indexer` are abstract classes because the framework manages their execution: stages are invoked per-document by the Worker's processing loop, and indexers are driven by a message consumption loop with batching logic. Those base classes embed that execution machinery.

**What `AbstractConnector` does for you:**

- **Config validation** — The constructor calls `getSpec().validate(config)` using your class's `SPEC` field. Missing or unrecognized properties fail at startup.
- **Common config parsing** — Reads `name`, `pipeline`, `docIdPrefix`, and `collapse` from the config automatically.
- **Document ID prefixing** — Provides `createDocId(id)` which prepends the configured `docIdPrefix` to your raw IDs.
- **No-op lifecycle defaults** — `preExecute()`, `postExecute()`, and `close()` are provided as no-ops so you only override what you need.

**What you implement:**

| Method | Required | Purpose |
|---|---|---|
| `execute(Publisher publisher)` | Yes | Read from your data source and call `publisher.publish(doc)` for each document. |
| `preExecute(String runId)` | No | Setup work before execution (e.g., delete stale records from an index). |
| `postExecute(String runId)` | No | Post-success work (e.g., commit an index). Only called if `execute()` succeeds. |
| `close()` | No | Release resources. Always called, even if earlier methods threw. |

**The Publisher:**

The `Publisher` passed to `execute()` is your interface to the rest of the Lucille pipeline. Key things to know:

- **Thread-safe** — `publish()` can be called from multiple threads concurrently. If your data source supports parallel reads (e.g., fetching pages from an API, reading from multiple partitions), you can spawn threads inside `execute()` and have each one call `publisher.publish(doc)` independently. No synchronization on your part is needed.
- **Backpressure** — When `publisher.maxPendingDocs` is configured, `publish()` blocks automatically if the number of in-flight documents exceeds the threshold. This prevents a fast connector from overwhelming the pipeline. You don't need to implement throttling yourself.
- **Do not reuse documents after publishing** — Once you call `publisher.publish(doc)`, the document may be picked up by a worker thread immediately. Do not read from or write to the document after publishing it.
- **Multi-threaded cleanup** — If your connector uses multiple publishing threads, each thread should call `publisher.preClose()` when it is done publishing. This releases per-thread resources inside the publisher. Single-threaded connectors do not need to call `preClose()`.
- **Collapsing mode** — If your connector sets `collapse: true` in its config, the publisher combines consecutive documents with the same ID into a single document with multi-valued fields. This is useful when your source emits multiple rows per logical record (e.g., a denormalized SQL join). Call `publisher.flush()` at the end of `execute()` if you use collapsing mode, to ensure the last held document is published.

**What you declare:**

| Field | Required | Purpose |
|---|---|---|
| `public static final Spec SPEC` | Yes | Declares the legal config properties for your connector. |

Your constructor must call `super(config)` — this triggers SPEC validation and parses the common connector properties.

**What's in `config`:** The `Config` passed to your Connector constructor contains only the properties defined inside your connector's config block — the `{ ... }` element from the `connectors` list. It does not contain the full Lucille config. You read your parameters directly: `config.getString("sourceUri")`. Your SPEC should declare only the properties that belong to your connector.

See [Control Flow: Pre- and Post-Connector Actions]({{< relref "docs/reference/control-flow" >}}) for the full lifecycle contract, including what happens when each method throws.

---

## Connector Skeleton

Every connector must follow the [Javadoc Standards]({{< relref "docs/developer-guide/javadocs" >}}).

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
  }
    
  @Override
  public void close() throws ConnectorException {
    // Optional: Close network or file handlers.
  }
}
```

## Unit Testing

Connector tests verify that `execute()` publishes the expected documents. The standard pattern is: create a `TestMessenger`, wrap it in a `PublisherImpl`, instantiate your connector, call `execute()`, and assert on the documents captured by the messenger.

### The basic pattern

```java
public class MyConnectorTest {

  @Test
  public void testExecute() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("MyConnectorTest/config.conf");
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    Connector connector = new MyConnector(config);
    connector.execute(publisher);

    List<Document> docs = messenger.getDocsSentForProcessing();
    assertEquals(10, docs.size());
    assertEquals("expected-value", docs.get(0).getString("title"));
  }
}
```

`TestMessenger` captures all documents published during `execute()`. Use `messenger.getDocsSentForProcessing()` to retrieve them for assertion.

### Mocking external services

Connectors that talk to external systems (databases, APIs, search engines) should inject a mock client rather than making real network calls. The common pattern is a constructor overload that accepts the client:

```java
// Production constructor — creates a real client
public MyConnector(Config config) {
  this(config, createRealClient(config));
}

// Test constructor — accepts a mock
public MyConnector(Config config, MyClient client) {
  super(config);
  this.client = client;
}
```

In tests, pass a Mockito mock:

```java
@Test
public void testWithMockClient() throws Exception {
  Config config = ConfigFactory.parseResourcesAnySyntax("MyConnectorTest/config.conf");
  MyClient mockClient = mock(MyClient.class);
  when(mockClient.query(any())).thenReturn(testData);

  TestMessenger messenger = new TestMessenger();
  Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");
  Connector connector = new MyConnector(config, mockClient);
  connector.execute(publisher);

  assertEquals(5, messenger.getDocsSentForProcessing().size());
}
```

`SolrConnector` follows this pattern — its test constructor accepts a `SolrClient` mock.

### Testing preExecute and postExecute

Test lifecycle methods directly when your connector overrides them:

```java
@Test
public void testPreAndPostActions() throws Exception {
  Config config = ConfigFactory.parseResourcesAnySyntax("MyConnectorTest/actions.conf");
  MyClient mockClient = mock(MyClient.class);
  Connector connector = new MyConnector(config, mockClient);

  connector.preExecute("run1");
  verify(mockClient, times(1)).deleteByQuery("runId:run1");

  connector.postExecute("run1");
  verify(mockClient, times(1)).commit();
}
```

### Testing error cases

Verify that your connector throws `ConnectorException` when it should:

```java
@Test(expected = ConnectorException.class)
public void testExecuteFailsOnBadSource() throws Exception {
  Config config = ConfigFactory.parseResourcesAnySyntax("MyConnectorTest/badPath.conf");
  TestMessenger messenger = new TestMessenger();
  Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");
  Connector connector = new MyConnector(config);
  connector.execute(publisher);
}
```

### Test config files

Store configs under `src/test/resources/MyConnectorTest/`. Each config contains just the connector's config block:

```hocon
{
  name: "test-connector"
  class: "com.kmwllc.lucille.connector.MyConnector"
  pipeline: "pipeline1"
  sourceUri: "src/test/resources/MyConnectorTest/test-data.csv"
}
```

### What to test

- **Document output** — correct number of documents, correct field values, correct IDs (including `docIdPrefix` behavior).
- **Each parameter** — at least one test path per required and optional parameter.
- **Error handling** — bad configs, unreachable sources, malformed data.
- **Lifecycle methods** — if you override `preExecute()` or `postExecute()`, test them independently.
- **Edge cases** — empty sources, sources with one record, large sources.

For full pipeline integration tests (running connectors through pipelines and indexers end-to-end in memory), see [Testing Pipelines]({{< relref "docs/developer-guide/testing" >}}).
