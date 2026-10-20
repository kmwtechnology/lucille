---
title: AddRandomNestedField
weight: 50
date: 2025-06-09
description: Writes a JSON array of objects to a field, filling each object from document fields or generator stages.
---

`com.kmwllc.lucille.stage.AddRandomNestedField`

Writes a JSON array of objects to `targetField`. Each object is built from `entries`, a mapping of dotted destination paths (inside the object) to sources. A source is either a field on the document or the key of a generator in `generators`. If the document has the source field, its value is copied (as JSON). Otherwise, if a generator has that key, the generator stage is run to produce a value. Useful for testing nested document structures.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `targetField` | String | Yes | Field the array of objects is written to. Must not be a reserved field. |
| `entries` | Map\<String, String\> | Yes | Destination path inside each object to source (a document field name or a generator key). Must not be empty. Paths are split on `.`; empty segments and duplicate paths fail at startup. Numeric segments are treated as array indices. |
| `numObjects` | Integer | No | Fixed number of objects per document. Must be positive. Cannot be combined with `minNumObjects`/`maxNumObjects`. If none of the three is set, one object is created. |
| `minNumObjects` | Integer | No | Minimum (inclusive) of a random number of objects per document. Must be positive and set together with `maxNumObjects`. |
| `maxNumObjects` | Integer | No | Maximum (inclusive) of a random number of objects per document. Must be positive, not less than `minNumObjects`, and set together with it. |
| `generators` | Map\<String, Object\> | No | Generator stage configs, keyed by the name used as a source in `entries`. Each config must include `class`. Generators are started with the stage. |

## Behavior

- A generator is used only when the document does not have a field with the source name. It runs once per entry per object, on a scratch document, not the document being processed.
- The generator's `fieldName` is always replaced with an internal temporary field, so any `fieldName` you set on a generator is ignored. A generator must therefore be a stage that accepts `fieldName` and writes its value there (such as the AddRandom* stages); any other stage fails validation at startup.
- If a source is not on the document and has no matching generator, the document fails with a `StageException`.
- If a value resolves to null, that entry is skipped with a warning.
- If `targetField` is still absent afterwards (nothing was written), it is set to an empty array.

## Example

```hocon
{
  name: "addPeople"
  class: "com.kmwllc.lucille.stage.AddRandomNestedField"
  targetField: "people"
  minNumObjects: 1
  maxNumObjects: 3
  entries: {
    "tag": "tagGen"
    "info.age": "ageGen"
  }
  generators: {
    tagGen: {
      class: "com.kmwllc.lucille.stage.AddRandomString"
      rangeSize: 100
    }
    ageGen: {
      class: "com.kmwllc.lucille.stage.AddRandomInt"
      rangeStart: 18
      rangeEnd: 90
    }
  }
}
```
