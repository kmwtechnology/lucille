---
title: ComputeFieldSize
weight: 170
date: 2025-06-09
description: Stores the length in bytes of a byte array field.
---

`com.kmwllc.lucille.stage.ComputeFieldSize`

Stores the length in bytes of a byte array field. Documents without `source` are skipped. If `source` is multi-valued, only the first value is measured. A value that is not a byte array fails the Document with a `StageException`; a string value is read as Base64 and the decoded length is stored, so a non-Base64 string fails.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | String | Yes | Field to measure. |
| `dest` | String | Yes | Destination field for the size in bytes, as an Integer. Overwrites any existing value. |
