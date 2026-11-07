---
title: Extending Lucille
weight: 10
date: 2026-11-02
description: >
  When you need to extend Lucille, which components you can build, which framework classes you use as they are, and what is internal.
---

## You may not need to extend Lucille

Lucille is designed to cover a wide range of ingestion use cases without custom code. Pipelines are driven by configuration: if your pipeline is made of built-in connectors, stages, and indexers, you write a config file and no Java. See [Writing a Config]({{< relref "docs/ingest-design/writing-a-config" >}}).

When the built-in stages don't do what you need, you can often run your own logic inside a pipeline through one of the scripting stages, without building or packaging a component:

- [ApplyJavascript]({{< relref "docs/ingest-design/stages/apply_javascript" >}}) runs a JavaScript script against each document.
- [EmbeddedPython]({{< relref "docs/ingest-design/stages/embedded_python" >}}) runs Python inside the JVM.
- [ExternalPython]({{< relref "docs/ingest-design/stages/external_python" >}}) runs Python in a separate process, so you can use any Python library.
- [ApplyJSONata]({{< relref "docs/ingest-design/stages/apply_jsonata" >}}) transforms documents with a JSONata expression.

Build a component when a script isn't enough: when you need a new data source or destination, a new file format, or enrichment logic that is better written, tested, and reused as Java.

## Extension points

These are the component types you can build. You reference each one by its fully qualified class name in the config; Lucille instantiates it reflectively. See [Where Your Code Lives]({{< relref "docs/developer-guide" >}}) for how to put your classes on the classpath.

| Component | Purpose | You implement | Configured with | Guide |
|---|---|---|---|---|
| **Connector** | Acquire documents from a custom source. | `Connector`, usually by extending `AbstractConnector` | `class` in a `connectors` entry | [Developing Connectors]({{< relref "docs/developer-guide/developing-connectors" >}}) |
| **Stage** | Enrichment or transformation logic, in Java, that no existing stage provides. | Extend `Stage` | `class` in a `stages` entry | [Developing Stages]({{< relref "docs/developer-guide/developing-stages" >}}) |
| **Indexer** | Send documents to a custom destination. | Extend `Indexer` | `indexer.class` | [Developing Indexers]({{< relref "docs/developer-guide/developing-indexers" >}}) |
| **FileHandler** | Parse a custom file format, splitting a file into one or more documents, during a `FileConnector` traversal or in `ApplyFileHandlers`. | `FileHandler`, usually by extending `BaseFileHandler` | `class` in a `fileHandlers` entry, keyed by file extension | [Developing File Handlers]({{< relref "docs/developer-guide/developing-file-handlers" >}}) |
| **FileContentFetcher** | Resolve a file path to its content in a custom way, for stages such as `FetchFileContent` and `ApplyFileHandlers`. | `FileContentFetcher` | `fetcherClass` on the stage | [Developing File Handlers]({{< relref "docs/developer-guide/developing-file-handlers" >}}) |
| **StorageClient** | Let `FileConnector` traverse, and stages such as `FetchFileContent` read from, a custom storage backend, or replace a built-in cloud client with your own implementation. | `StorageClient`, usually by extending `BaseStorageClient`, with a public constructor that takes a `Config` | `class` in a `storageClients` entry, keyed by URI scheme | [Developing Storage Clients]({{< relref "docs/developer-guide/developing-storage-clients" >}}) |
| **RetryCounter** | Track documents that keep failing in a Kafka deployment, in a store of your choice, so they can go to the dead letter queue. | `RetryCounter`, with a public constructor that takes the full `Config` | `worker.retryCounter.class` | [Worker: Poison Pills]({{< relref "docs/architecture/components/worker#poison-pills" >}}) |

Every component declares a [SPEC]({{< relref "docs/developer-guide/spec-validation" >}}) and follows the [Javadoc Standards]({{< relref "docs/developer-guide/javadocs" >}}). See [What the Framework Gives You]({{< relref "docs/developer-guide/framework-benefits" >}}) for what the base classes do for you, and [Testing Pipelines]({{< relref "docs/developer-guide/testing" >}}) for testing conventions.

Extend the base classes and interfaces above, not Lucille's built-in components. Classes such as `FileConnector`, `SolrIndexer`, or a built-in stage are meant to be configured; their internals may change between releases, so subclassing them is not supported. This includes the built-in StorageClients (`S3StorageClient`, `AzureStorageClient`, `GoogleStorageClient`, and `LocalStorageClient`), which expose the cloud SDKs' types. To read from a storage system the built-ins don't cover, or to handle a built-in scheme differently, write your own StorageClient on `BaseStorageClient` and register it for the scheme in `storageClients`.

## Classes you use but don't extend

Your components and applications interact with these classes. They are public API: use them as they are, without subclassing or implementing them.

**Running Lucille**
- `Runner`: runs a pipeline from a config, through `main`, `run`, `runAndLogResult`, or `runInTestMode`. Its `-validate` option checks a config and exits without running.
- `Runner.RunType`: the kind of run: `LOCAL`, `TEST`, `EXTERNAL` (Kafka), or `DISTRIBUTED`.
- `RunResult`: the outcome of a run.
- `TestMessenger`: the recording of a test-mode run, and what you pass to an `IndexerContext` when unit testing an Indexer. Its supported API is `getDocsSentForProcessing()`, `getDocsSentForIndexing()`, and `getSentEvents()`, plus `sendForIndexing()` to queue documents for an Indexer under test; the other messenger methods it implements are internal.

**Documents and configuration**
- `Document`: the unit of data that flows through a pipeline, created with `Document.create`.
- `UpdateMode`: how a write treats a field's existing values.
- `Spec` and `SpecBuilder`: declare and validate a component's configuration.
- `ConfigUtils`: helpers for reading optional config values.

**Connector support**
- `Publisher`: passed to a connector's `execute()`; you call `publish()` for each document.

**Indexer support**
- `IndexerContext`: what the framework provides to an Indexer's constructor, apart from its config. Your constructor takes it and passes it to `super`. It includes a messenger, which is internal: the base class does all polling, event sending, and offset handling, so your Indexer never calls it.
- `IndexerFailure`: what `sendToIndex()` returns for each document that could not be indexed.

**Base classes and types for file handling**
- `BaseFileHandler`: the base class for a FileHandler.
- `BaseStorageClient`, `FileReference`, `BaseFileReference`, and `TraversalParams`: the types a StorageClient works with.

**Exceptions**
- `StageException`, `ConnectorException`, `IndexerException`, `IndexerRetryableException`, `FileHandlerException`, and `DocumentException`: throw these from your components to signal failure. The framework decides what happens to the document or the run.

**Testing**
- `StageFactory`: creates and starts a stage from a config in a unit test.

**Utilities**
- `SSLUtils`, `HttpClientConfigUtils`, and `HttpHostUtils`: TLS and HTTP client setup for components that call external services.

## Everything else is internal

Any class not named on this page is internal to Lucille, even when it is public in Java. That includes the messengers, `Worker`, `WorkerPool`, `Pipeline`, `Batch`, `PublisherImpl`, `RunnerManager`, and the rest of `com.kmwllc.lucille.util`. Don't call, extend, or depend on them: they can change in any release without notice.

**Custom messengers are not supported.** The messenger interfaces (`IndexerMessenger`, `WorkerMessenger`, and `PublisherMessenger`) and their implementations are internal, and they can gain, change, or lose methods in any release. You will meet a messenger only inside `IndexerContext` and in unit tests through `TestMessenger`, as described above. To change where documents are sent, build an Indexer. To change how they are acquired, build a Connector.
