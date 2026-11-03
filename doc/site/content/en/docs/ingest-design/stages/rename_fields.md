---
title: RenameFields
weight: 560
date: 2025-06-09
description: Renames fields by mapping old names to new names.
---

`com.kmwllc.lucille.stage.RenameFields`

Renames fields by mapping old names to new names. A source field that is missing from the Document is skipped.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fieldMapping` | Map\<String, String\> | Yes | Map of old field name to new field name. Must contain at least one entry, or the stage fails at startup. |
| `updateMode` | String | No | How to handle a destination field that already exists: `overwrite`, `append`, or `skip`. With `skip`, the destination is left unchanged and the source field is still removed. Any other value fails at startup. Default: `overwrite`. |
| `applyToChildren` | Boolean | No | When `true`, renaming is also applied recursively to all attached child documents (including children of children). Stage conditions control whether the stage runs but are not re-evaluated per child. Default: `false`. |

```hocon
{ class: "com.kmwllc.lucille.stage.RenameFields", fieldMapping: { old_field: new_field } }
```
