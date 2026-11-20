---
title: "Config Property Naming"
weight: 25
date: 2026-11-08
description: >
  Conventions and recommendations for naming the configuration properties of Connectors, Stages, Indexers and File Handlers.
---

## Overview

A component's configuration property names are part of its public interface. People type them into configs, read them in error messages, and look them up in the docs. Names that follow a common pattern let someone who knows one component guess the properties of the next.

This page describes the conventions Lucille's built-in components follow. They are **conventions and recommendations, not strict requirements**. Nothing in the build or in Spec validation enforces them. New components, built-in or your own, should follow them unless there is a good reason not to. When a convention doesn't fit, choose the name that is clearest for that component's users, and say why in its Javadoc.

The most basic convention is that every property name is in **camel case**: it starts lowercase, and each word after the first starts with a capital letter, as in `idField`, `batchTimeoutMs` and `includeFields`. Acronyms and compound words follow the same rule (see [Capitalization and Acronyms](#capitalization-and-acronyms)).

Four principles sit behind the more specific conventions:

- **Same concept, same name.** If two components have a setting that does the same thing, give it the same name, even if one of them came first with a different one.
- **Say what the property holds.** A reader should be able to tell from the name whether a property is a field name or a literal value, a single name or a list, and which unit a number is in.
- **Follow the majority.** When existing components disagree, follow the form most of them already use.
- **Spell words out.** Write `source`, `length` and `separator`, not `src`, `len` and `sep`. There are three exceptions, all well established and so common that brevity is preferable: `dest`, for a stage's output field, rather than `destination`; `doc` or `docs` inside a longer name (`docIdFormat`), rather than `document` or `documents`; and `num` in a count (`numDocs`, `minNumTerms`; see [Numbers and Percentages](#numbers-and-percentages)), rather than `number`. A property that sets a setting of another system keeps that system's name for it (see [Settings of External Systems](#settings-of-external-systems)).

## Field Names: `source`, `dest`, `fields`

Most Stages read one or more document fields and write one or more document fields.

| Property | Use it for | Examples |
|---|---|---|
| `source` | The field or fields the stage reads | `ParseJson`, `ParseFloats`, `XPathExtractor`, `Base64Decode` |
| `dest` | The field or fields the stage writes | `Timestamp`, `Contains`, `TruncateField`, `AddRandomInt` |
| `fields` | The fields a stage modifies in place, with no separate output | `DeleteFields`, `TrimWhitespace` |
| `fieldMapping` | A map from what the stage reads to the field or fields it writes | `CopyFields`, `RenameFields`, `EmitNestedChildren` (source field to destination field), `XPathExtractor` (XPath expression to fields) |

Avoid synonyms for these: `src`, `inputField`, `outputField`, `destination`, `destField`, `targetField`. `dest` is short for "destination", an exception to [spelling words out](#overview): it is well established, and so common that the short form is preferable. Avoid names that don't say whether the field is read or written, such as `field` or `fieldName`, when the stage has both an input and an output.

Don't put the content type in the name of a stage's main input. `XPathExtractor` reads XML from `source`, not `xmlField`; parsing XML is the stage's job, not something the key needs to say.

**When `source` and `dest` don't fit.** `source` and `dest` are for a stage that reads a field and writes the result. Some components have a different shape:
- they take their input in one of several forms
- they write several outputs
- they create documents rather than transform a field

For these, name each field for what it holds, with the `Field` suffix described below:
- `TextExtractor` reads a file path from `filePathField` or raw bytes from `byteArrayField`. It writes the extracted text to `textField` and the metadata names to `fieldNamesField`. A bare `dest` there wouldn't say which of its outputs it is.
- `XMLFileHandler` puts each element's XML in `xmlField`.
- `CSVFileHandler` puts the file name in `filenameField`.

## Properties That Name a Document Field: the `Field` Suffix

When a property names a document field that holds one particular piece of information, name it after that information and end it with `Field`: `idField`, `languageField`, `filenameField`, `idOverrideField`, `indexOverrideField`.

The suffix matters most when a component also takes literal values. Compare the two blocks of Indexer deletion settings:

```hocon
indexer {
  deletionMarker {
    field: "is_deleted"             # a field name
    value: "true"                   # a literal value
  }
  deleteByQuery {
    fieldNameField: "delete_query_field"   # a field whose value is a field name
    valueField: "delete_query_value"       # a field whose value is the value to match
  }
}
```

`value` is a value, and `valueField` names a field that holds a value. A reader can tell them apart from the names alone.

## File Paths

| Pattern | Use it for | Examples |
|---|---|---|
| `xxxPath` | A file or location in the config | `scriptPath`, `dictionaryPath`, `tikaConfigPath`, `modelDownloadPath`, `serviceKeyPath` |
| `path`, `paths` | The location a connector reads from | `ParquetConnector.path`, `FileConnector.paths` |
| `filePathField` | A document field that holds a file path | `TextExtractor`, `ApplyJSoup`, `FetchFileContent`, `ApplyOCR` |
| `filenameField` | A document field that holds a file name | `CSVFileHandler`, `VideoFileHandler` |

`filenameField` and `filePathField` follow the same capitalization rule: "filename" is one word and "file path" is two (see [Capitalization](#capitalization-and-acronyms)).

Put the role first and `Path` last (`modelDownloadPath`, not `pathToStoreModel`), and say what the location is for when a shorter name could mislead. `JlamaEmbed`'s `modelDownloadPath` is the directory the model is downloaded to. A plain `modelPath` would read as the path of a model file to load.

A bare `path` is also used for JSON and XML paths in some components, so use `filePathField`, not `pathField`, for a field holding a file path.

## Units

**Every numeric duration names its unit.** End the name in `Ms` for milliseconds or `Secs` for seconds: `connectTimeoutMs`, `batchTimeoutMs`, `pollIntervalMs`, `timeoutSecs`, `maxProcessingSecs`. Write `Ms`, not `Millis`. Without the suffix, a reader has to look up whether `timeout: 30` means 30 seconds or 30 milliseconds.

A property that takes a duration *string*, such as `"1h"` or `"30s"`, doesn't need a suffix, because the unit is in the value. `RSSConnector.runDuration` and `FileConnector.lastModifiedCutoff` work this way.

**Sizes name their unit too.** Use `Bytes` for a byte count (`maxSizeBytes`, `maxRecordBytes`) and `Length` for a character count (`maxLength`, `minLength`). Avoid a bare `maxSize`, which doesn't say which.

**Name what the number measures, not just its unit.** `log.intervalSecs` says what the number is; `log.seconds` didn't.

## Numbers and Percentages

| Pattern | Use it for | Examples |
|---|---|---|
| `percentX` | A percentage on a 0–100 scale | `percentTrue`, `percentToDrop`, `percentOverlap` |
| `minNumX`, `maxNumX`, `numX` | A count of things | `minNumTerms`, `maxNumObjects`, `numDocs` |
| `maxX` | An upper limit | `maxRetries`, `maxConcurrentPaths`, `maxLength` |

A property named as a percentage should take a percentage. If a setting is really a probability between 0 and 1, don't call it a percentage.

## Booleans

Name a boolean for what it turns on, as a verb phrase or adjective: `ignoreCase`, `overwriteFile`, `enableOcr`, `dropOriginal`, `appendThreadName`, `useXml`. Don't add a `should` prefix, and avoid a bare word that reads as either a noun or a verb. Write `logDocuments`, not `shouldLog` or `log`. A bare `log` could also be mistaken for the top-level `log` block. The Elasticsearch and OpenSearch indexers' `update` is an exception: it is well established, and it names the kind of request the indexer sends, so it can't be read any other way.

## Capitalization and Acronyms

Camel case goes by words. A compound that is normally written as one word counts as one word, so "filename" gives `filenameField` and "file path" gives `filePathField`. Likewise, "timestamp" is one word and "time zone" is two (`timeZoneId`).

Write acronyms in camel case, as if they were ordinary words: `idField`, `docIdFormat`, `hostUrl`, `rssUrl`, `requireJson`, `useXml`, `enableOcr`, `preSql`. `URL`, `JSON` and `ID` in all capitals make the word boundaries hard to see and are easy to mistype.

## Include and Exclude Lists

| Pattern | Use it for | Examples |
|---|---|---|
| `includeFields`, `excludeFields` | Lists of document field names | Indexer, `JsonFileHandler`, `Print` |
| `includeX`, `excludeX` | Lists of names of some other kind | `TextExtractor.includeMetadata`, `excludeMetadata` |
| `includes`, `excludes` | Lists of patterns | `FileConnector` path regexes |

Say what the list holds, and avoid the `whitelist` and `blacklist` metaphor.

## Maps and Lists

Use the plural for a list (`fields`, `paths`, `formatStrings`, `retryableStatusCodes`) and `xxxMapping` for a map whose keys are what the component reads and whose values are where it writes (`fieldMapping`). Don't call such a map `fieldsToCopy`: that reads as a list.

Keep that direction for anything named `xxxMapping`. If a map runs the other way, name it for what its values hold instead. `TextExtractor.metadataHintFields` maps Tika metadata names to the document fields that supply hints to Tika before parsing. Calling it `metadataMapping` would suggest it maps Tika's extracted metadata to output fields.

## Shared Settings

When a setting already exists in another component, reuse its name and meaning instead of inventing a new one. Some shared settings:

| Setting | Names | Used by |
|---|---|---|
| Retries | `maxRetries`, `retryWaitDurationMs`, `retryMaxWaitDurationMs`, `retryableStatusCodes` | Indexer, `FetchUri` |
| HTTP timeouts | `connectTimeoutMs`, `socketTimeoutMs` | Elasticsearch and OpenSearch indexers, `FetchUri` |
| Cloud storage credentials | `s3`, `gcp` and `azure` blocks, with keys such as `accessKeyId`, `secretAccessKey`, `serviceKeyPath` | `FileConnector`, `ParquetConnector`, the stages that fetch file content |
| Model names | `modelName` | `OpenAIEmbed`, `JlamaEmbed`, `PromptOllama` |

## Settings of External Systems

When a property sets a specific, named setting of another system, such as a Kafka client property, a ZooKeeper connect string or a SolrJ client parameter, use that system's name for it, converted to camel case word for word: Kafka's `max.request.size` becomes `maxRequestSize`, and `min.insync.replicas` becomes `minInsyncReplicas`. Keep the external system's words, abbreviations and units, even where they differ from the conventions on this page, so that someone who knows that system recognizes the setting and can look it up in that system's documentation. Examples: `kafka.bootstrapServers`, `kafka.maxRequestSize`, `solr.zkHosts`, `zookeeper.connectString`.

A prefix that says which part of Lucille the setting applies to is fine (`eventTopicReplicationFactor`). The convention does not apply when a property is only related to a concept in another system without corresponding to one of its settings; name such properties by the conventions on this page.

## Nested Blocks

Put settings that only make sense together in a nested block, and give the keys inside short names, since the block already says what they belong to: `deletionMarker { field, value }`, `s3 { accessKeyId, secretAccessKey }`. Declare the block with `optionalParent` or `requiredParent` and make its keys required in the block's Spec (see [Nested Specs]({{< relref "docs/developer-guide/spec-validation#nested-specs-parent-specs" >}})). Validation then rejects a block with a key missing, so the component doesn't need its own "set both or neither" check.

If another component already has a block for the same thing, reuse its name and shape. `EmitDocsToDeleteByPrefix` takes the same `deletionMarker` block as the Indexer, because the two must agree.

## Renaming a Property

Lucille doesn't keep old property names as aliases. Because Spec validation [rejects unrecognized properties]({{< relref "docs/developer-guide/spec-validation#unrecognized-property-rejection" >}}), a config that still uses an old name fails at startup with an error that names the property, so the user knows what to change. A property that is still accepted but silently ignored would be worse.

If you rename a property:

- Update the Spec, the Javadoc "Config Parameters" list, the docs, example configs and tests together.
- If the meaning of the value changes, for example from a 0–1 fraction to a 0–100 percentage, change the name too, so that an old value can't be read with the new meaning.
- List the rename in the release notes.

## Checklist for a New Component

- Every property name is in camel case.
- If it reads a field and writes the result, the fields are `source` and `dest`, or `fields` if it modifies them in place. Otherwise, each field is named for what it holds.
- Every property that names a document field ends in `Field`, unless it is `source`, `dest` or `fields`.
- Every numeric duration ends in `Ms` or `Secs`, and every size names its unit.
- Acronyms are in camel case.
- Settings that already exist elsewhere use the same name.
- A property that sets a setting of an external system uses that system's name for it.
- Settings that must be set together are in a nested block with required keys.
