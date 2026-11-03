---
title: HashFieldValueToBucket
weight: 370
date: 2025-06-09
description: Assigns each Document to one of a list of bucket labels by hashing, and writes the label to a field.
---

`com.kmwllc.lucille.stage.HashFieldValueToBucket`

Assigns each Document to one of the labels in `buckets` and writes that label to `dest`. The bucket is chosen from the hash code modulo the number of buckets, so the same input always lands in the same bucket.

The hash is computed from the value of `fieldName` (or from the Document ID when `fieldName` is `id`). Documents without the field are left unchanged.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fieldName` | String | Yes | Field whose value is hashed. Use `id` to bucket by Document ID. |
| `dest` | String | Yes | Field to write the bucket label to. |
| `buckets` | List\<String\> | Yes | Bucket labels. Must not be empty. |
