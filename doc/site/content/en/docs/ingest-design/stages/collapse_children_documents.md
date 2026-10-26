---
title: CollapseChildrenDocuments
weight: 160
date: 2025-06-09
description: Copies field values from a Document's children onto the parent as multi-valued fields.
---

`com.kmwllc.lucille.stage.CollapseChildrenDocuments`

Copies field values from a Document's attached children onto the parent, optionally dropping the children afterwards. Documents without children are left unchanged.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fieldsToCopy` | List\<String\> | Yes | Fields to copy from each child onto the parent. Values are appended to the parent field, including any values the parent already had. Fields missing on a child are skipped. All values of a multi-valued child field are copied. |
| `dropChildren` | Boolean | Yes | Whether to remove the children from the parent after copying. |

The parent field becomes multi-valued once it holds more than one value. If the parent did not have the field and only one child supplies a single value, the field stays single-valued.

Commonly used to flatten the child Documents produced by the `otherSQLs` joins of the [Database Connector]({{< relref "docs/ingest-design/connectors/database_connector#multi-query-joins" >}}).
