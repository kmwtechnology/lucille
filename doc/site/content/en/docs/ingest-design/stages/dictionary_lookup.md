---
title: DictionaryLookup
weight: 250
date: 2025-06-09
description: Looks up field values in a term dictionary and adds matched entries as field values.
---

`com.kmwllc.lucille.stage.DictionaryLookup`

Looks up field values in a term dictionary and adds matched entries as field values.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | List\<String\> | Yes | Fields whose values are used as lookup keys. |
| `dest` | List\<String\> | Yes | Destination fields for lookup results. |
| `dictPath` | String | Yes | Path to the dictionary file. |
