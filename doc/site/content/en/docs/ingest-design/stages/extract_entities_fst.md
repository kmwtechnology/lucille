---
title: ExtractEntitiesFST
weight: 330
date: 2025-06-09
description: Performs named entity recognition using a finite-state transducer dictionary.
---

`com.kmwllc.lucille.stage.ExtractEntitiesFST`

Performs named entity recognition using a finite-state transducer dictionary.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | String | Yes | Field containing text to extract entities from. |
| `dest` | String | Yes | Destination field for extracted entity values. |
| `dictionaryPath` | String | Yes | Path to the FST dictionary file. |
