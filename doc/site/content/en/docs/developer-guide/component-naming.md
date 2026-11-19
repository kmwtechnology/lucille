---
title: "Component Naming"
weight: 24
date: 2026-11-19
description: >
  Conventions and recommendations for naming the classes of Stages, Connectors, Indexers, File Handlers and Storage Clients.
---

## Overview

A component's class name is part of its public interface. Users put the fully qualified class name in a config's `class` property, see it in error messages and logs, and look it up in the docs. Names that follow a common pattern let someone who knows one component guess the name of the next. Renaming a component breaks every config that uses it, so a name is hard to change later and worth getting right from the start.

This page covers class names. The `name` property of a stage, which labels one stage in one pipeline, is a user's choice and has no conventions. For the names of configuration properties, see [Config Property Naming]({{< relref "docs/developer-guide/config-naming" >}}).

Like the property conventions, these are **conventions and recommendations, not strict requirements**. Nothing in the build enforces them. New components, built-in or your own, should follow them unless there is a good reason not to.

Stages are named for what they do, with a verb first, because a stage is an action applied to each document. The other components are named for what they are, with their type last, because each one is a thing that connects Lucille to a source, a destination, a file format or a kind of storage. The rules in [All Components](#all-components) apply to both.

## All Components

### Spell Product Names the Way Their Owners Do

Write a product name the way its owner writes it, fitted to camel case: `OpenSearch`, `Elasticsearch`, `JavaScript`, `JSONata`, `OpenAI`, `OpenNLP`, `XPath`, `Jlama`, `Ollama`, `Jsoup` and `S3`. jsoup is lowercase, so at the start of a word it becomes `Jsoup`, as in its own class `org.jsoup.Jsoup`. `OpenSearchIndexer` and `ElasticsearchIndexer` follow this rule.

### Write Acronyms in Camel Case

Write acronyms as if they were ordinary words: `ParseJson`, `FetchUri`, `ApplyOcr`, `ExtractEntitiesFst`, `CsvIndexer`, `RssConnector`, `JsonFileHandler`. Acronyms in all capitals make the word boundaries hard to see, especially next to another word or acronym: `HttpUrl` reads more easily than `HTTPURL`. Property names follow the same rule (see [Capitalization and Acronyms]({{< relref "docs/developer-guide/config-naming#capitalization-and-acronyms" >}})), and so do the Google Java Style Guide and newer JDK APIs such as `java.net.http.HttpClient`.

Product names are not acronyms. A product name keeps its owner's spelling, even where it has capitals in a row (`JSONata`, `OpenAI`, `S3`).

### Spell Words Out

Spell words out, with the same exceptions as for property names (see [Config Property Naming]({{< relref "docs/developer-guide/config-naming#overview" >}})), such as `Docs` inside a longer name (`EmitDocsToDeleteByPrefix`). A well-established technical term may also be used in its usual short form: `Nop`, for "no operation", in `NopIndexer`.

## Stage Names

### Start with a Verb

A stage name is an imperative verb phrase: the verb, then what it acts on, then any qualifier, as in `CopyFields`, `ParseDate`, `DetectLanguage` and `ExtractEntitiesFst`. A stage does something to a document, and the name says what.

A bare verb is fine when what it acts on is obvious: `Concatenate`, `Print`.

Don't name a stage with an agent noun (`FieldCopier`, `DateParser`) or a bare noun (`Language`, `Teaser`). Write `CopyFields`, `ParseDate`, `DetectLanguage` and `CreateStaticTeaser`.

Lookup stages are the one exception (see [Lookup Stages](#lookup-stages)).

### Use One Verb per Kind of Job

Stages that do the same kind of job use the same verb:

| Verb | Use it when the stage | Examples |
|---|---|---|
| `Fetch` | Reads bytes from a location | `FetchFileContent` |
| `Query` | Runs a search against an external system | `QueryDatabase` |
| `Apply` | Runs an expression language, model or set of handlers | `ApplyRegex`, `ApplyJSONata`, `ApplyXPath`, `ApplyFileHandlers`, `ApplyOpenNLPNameFinders` |
| `Run` | Runs user-supplied code on each document | `RunPython`, `RunExternalPython`, `RunJavaScript` |
| `Add` | Writes a generated value to a field | `AddRandomInt`, `AddRandomString` |
| `Set` | Writes values given in the config to a field | `SetStaticValues` |
| `Extract` | Pulls values out of content | `ExtractEntities`, `ExtractFirstCharacter` |
| `Emit` | Produces child documents | `EmitNestedChildren`, `EmitDocsToDeleteByPrefix` |

A `Run` stage starts its interpreter, or for `RunExternalPython` its Python process, when the stage starts, not for each document.

Stages that do different jobs keep different verbs, even when they look alike. `ApplyRegex` extracts the matches of a regular expression and `ReplacePatterns` finds and replaces, so each has its own verb.

### Put a Provider After the Verb

When a stage name includes a service, product or library, it comes after the verb, either as the object (`PromptOllama`, `ApplyJSONata`) or after the object (`EmbedOpenAI`, not `OpenAIEmbed`).

You don't need to name a provider that the stage's package already names. The Pinecone plugin's `EmitDocsToDeleteByPrefix` doesn't say "Pinecone".

### Lookup Stages

A stage that gets entries by key from a named source is named for the source, followed by `Lookup`: `DictionaryLookup` looks up each source value in a dictionary file, and `ElasticsearchLookup` gets a document from Elasticsearch by its ID. This is the one stage name that doesn't start with a verb and puts the provider first. "Lookup" is an established name for the job, and the source is what tells one lookup stage from another.

A stage that runs a search, rather than getting entries by key, uses `Query` instead (`QueryOpenSearch`, `QueryDatabase`).

### Use Singular or Plural by the Job

- Use the plural when the stage works on a set by design (`DeleteFields`, `ReplacePatterns`, `DropValues`) or produces many values from one input (`ParseFloats`, `ExtractEntities`).
- Use the singular when the stage does one thing, even if it repeats it for several source fields (`ParseDate`, `NormalizeText`), or takes a single field (`TruncateField`).

### Leave Out Filler Words

- Don't end a name in `Stage`. Everything in `class` under `stages` is a stage.
- "Children" already means child documents in Lucille, so don't add "Documents" after it: `EmitNestedChildren`.

## Connector, Indexer, File Handler and Storage Client Names

### Qualifier, Then Type

These components are named with a qualifier followed by the component type: `<Qualifier>Connector`, `<Qualifier>Indexer`, `<Qualifier>FileHandler` or `<Qualifier>StorageClient`. Always include the type suffix. A connector and an indexer for the same system share the qualifier (`SolrConnector`, `SolrIndexer`).

The qualifier says what the component works with. Choose the most specific of these that is true:

1. **A product, service or format**, when the component works with that one: `KafkaConnector`, `SolrConnector`, `ParquetConnector`, `RssConnector`, `OpenSearchIndexer`, `PineconeIndexer`, `CsvIndexer`, `CsvFileHandler`, `XmlFileHandler`, `S3StorageClient`, `AzureStorageClient`.
2. **A kind of system or data**, when the component works with any system of that kind: `DatabaseConnector` reads from any database with a JDBC driver, `FileConnector` reads files from local and cloud storage, and `LocalStorageClient` reads the local file system.
3. **What the component does**, when it doesn't work with an external system at all, as with components for testing or generating data: `SequenceConnector` produces a sequence of empty documents, and `NopIndexer` sends nothing anywhere.

### Connectors

The qualifier names the source the connector reads from.

### Indexers

The qualifier names the destination the indexer writes to. It is also the indexer's type name, the value a user gives in `indexer.type`, so it must read well on its own: `OpenSearchIndexer` is selected with `indexer.type: "OpenSearch"`, and `NopIndexer` with `indexer.type: "Nop"`. Declare it with `@IndexerType`.

### File Handlers

The qualifier names the file format the handler parses: `CsvFileHandler`, `XmlFileHandler`, `JsonFileHandler`, `VideoFileHandler`. The built-in handlers are selected by file extension key, and a custom handler by `class`.

### Storage Clients

The qualifier names the storage service or provider: `S3StorageClient`, `AzureStorageClient`, `GoogleStorageClient`, `LocalStorageClient`. The built-in clients are selected by URI scheme, and a custom client by `class` in `storageClients`.

## Renaming a Component

Lucille doesn't keep old class names as aliases. An alias would be a second public class to maintain, and it would appear twice in the component list of the API and the Admin UI. A config that still uses an old name fails at startup because the class can't be found, for a stage with `Stage class not found: <name>`, so the user knows what to change.

If you rename a component:

- Rename its class, its test class and its test resource directory together, and update the docs, example configs and tests.
- For an indexer, the type name in `@IndexerType` changes with the class name.
- List the old and new names in the release notes.

## Checklist for a New Component

- Product names are spelled as their owners spell them, and other acronyms are in camel case.
- Words are spelled out, apart from the allowed abbreviations.
- For a stage:
  - The name starts with a verb, and uses the same verb as other stages that do the same kind of job. A lookup stage is named `<Source>Lookup` instead.
  - A provider name comes after the verb.
  - The name is singular or plural by what the stage does.
  - The name doesn't end in `Stage`.
- For a connector, indexer, file handler or storage client:
  - The name ends in its type: `Connector`, `Indexer`, `FileHandler` or `StorageClient`.
  - The qualifier names the product, service or format it works with; or the kind of system, if it works with any of that kind; or what it does, if it works with no external system.
