---
title: RandomVector
weight: 520
date: 2025-06-09
description: Generates a random float vector and sets it on a document field.
---

`com.kmwllc.lucille.stage.RandomVector`

Generates a random float vector and sets it on a document field. Useful for testing vector search pipelines.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `dest` | String | Yes | Destination field for the random vector. |
| `dimensions` | Integer | Yes | Number of dimensions in the vector. |
