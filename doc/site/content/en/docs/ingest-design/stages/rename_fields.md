---
title: RenameFields
weight: 560
date: 2025-06-09
description: Renames fields by mapping old names to new names.
---

`com.kmwllc.lucille.stage.RenameFields`

Renames fields by mapping old names to new names.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fieldMapping` | Map\<String, String\> | Yes | Map of old field name → new field name. |
| `updateMode` | String | No | How to handle a destination field that already exists: `overwrite` (default), `append`, or `skip`. |
| `applyToChildren` | Boolean | No | When `true`, renaming is also applied recursively to all attached child documents (including children of children). Stage conditions control whether the stage runs but are not re-evaluated per child. Default: `false`. |

```hocon
{ class: "com.kmwllc.lucille.stage.RenameFields", fieldMapping: { old_field: new_field } }
```
