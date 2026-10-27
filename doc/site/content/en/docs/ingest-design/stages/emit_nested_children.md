---
title: EmitNestedChildren
weight: 300
date: 2025-06-09
description: Emits a Document's attached child Documents as independent Documents, optionally copying fields from the parent.
---

`com.kmwllc.lucille.stage.EmitNestedChildren`

Emits the child Documents attached to a Document (for example by a connector or an earlier stage) as independent Documents, and removes them from the parent. Documents without children are left unchanged.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `dropParent` | Boolean | No | Drop the parent Document once its children are emitted. Default: `false`. |
| `fieldsToCopy` | Map\<String, String\> | No | Maps parent field names to child field names. Each listed parent field is copied onto every child. Fields missing on the parent are skipped. |
| `updateMode` | String | No | How copied fields are written when the child already has the field: `overwrite`, `append`, or `skip`. Default: `overwrite`. |
