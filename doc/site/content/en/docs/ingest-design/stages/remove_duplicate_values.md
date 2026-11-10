---
title: RemoveDuplicateValues
weight: 540
date: 2025-06-09
description: Removes duplicate values from multi-valued fields.
---

`com.kmwllc.lucille.stage.RemoveDuplicateValues`

Removes duplicate values from multi-valued fields. The first occurrence of each value is kept, in its original order.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fieldMapping` | Map\<String, String\> | Yes | Map of source field to the field the deduplicated values are written to. Use the source field name as the target to deduplicate in place. Must not be empty. |

## Behavior

- Fields that are missing or single-valued are skipped.
- When the target differs from the source, the source is not changed and the deduplicated values are added to the target. If the target is already a multi-valued field, they are appended after its existing values. If the target holds a single value, processing fails with an exception.

```hocon
{
  class: "com.kmwllc.lucille.stage.RemoveDuplicateValues"
  fieldMapping: {
    tags: "tags"
    authors: "unique_authors"
  }
}
```
