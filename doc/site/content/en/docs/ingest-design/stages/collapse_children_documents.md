---
title: CollapseChildrenDocuments
weight: 160
date: 2025-06-09
description: Copies field values from a Document's children onto the parent as multi-valued fields.
---

`com.kmwllc.lucille.stage.CollapseChildrenDocuments`

Copies field values from a Document's children onto the parent as multi-valued fields, optionally dropping the children afterwards.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fieldsToCopy` | List\<String\> | Yes | Fields to copy from each child onto the parent. |
| `dropChildren` | Boolean | Yes | Whether to remove the children from the parent after copying. |

Commonly used to flatten the child Documents produced by the `otherSQLs` joins of the [Database Connector]({{< relref "docs/ingest-design/connectors/database_connector#multi-query-joins" >}}).
