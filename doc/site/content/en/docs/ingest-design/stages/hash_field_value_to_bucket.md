---
title: HashFieldValueToBucket
weight: 370
date: 2025-06-09
description: Assigns each Document to one of a list of bucket labels by hashing, and writes the label to a field.
---

`com.kmwllc.lucille.stage.HashFieldValueToBucket`

Assigns each Document to one of the labels in `buckets` and writes that label to `dest`. The bucket is chosen from the hash code modulo the number of buckets, so the same input always lands in the same bucket.

**Note:** the hash is currently computed from the Document ID, not from the value of `fieldName`. `fieldName` must still be set to a non-empty value.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fieldName` | String | Yes | Intended input field for the hash. See the note above. |
| `dest` | String | Yes | Field to write the bucket label to. |
| `buckets` | List\<String\> | Yes | Bucket labels. Must not be empty. |
