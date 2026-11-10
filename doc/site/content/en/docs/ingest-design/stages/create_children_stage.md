---
title: CreateChildrenStage
weight: 210
date: 2025-06-09
description: Emits a fixed number of empty child documents for each document. Intended for testing.
---

`com.kmwllc.lucille.stage.CreateChildrenStage`

Emits `numChildren` child documents for each document it processes. Children have no fields; their IDs are `<parentId>_child1`, `<parentId>_child2`, and so on. Intended for testing only.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `numChildren` | Integer | No | Number of children to emit per document. Default: `3`. |
| `dropParent` | Boolean | No | If `true`, marks the parent document as dropped. Default: `false`. |
| `failAfter` | Integer | No | If set, throws a `RuntimeException` when asked for the next child after this many children have been emitted. Has no effect unless it is less than `numChildren`. |
| `dropChild` | Integer | No | If set, marks the child with this 1-based index as dropped. |
