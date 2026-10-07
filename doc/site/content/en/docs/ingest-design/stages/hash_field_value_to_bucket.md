---
title: HashFieldValueToBucket
weight: 370
date: 2025-06-09
description: Hashes a field's value and assigns the document to a numbered bucket.
---

`com.kmwllc.lucille.stage.HashFieldValueToBucket`

Hashes a field's value and assigns the document to a numbered bucket (useful for deterministic partitioning).

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | String | Yes | Field whose value is hashed. |
| `dest` | String | Yes | Field to write the bucket number to. |
| `numBuckets` | Integer | Yes | Total number of buckets. |
