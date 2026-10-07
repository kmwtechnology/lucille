---
title: ReplacePatterns
weight: 570
date: 2025-06-09
description: Performs pattern-based find-and-replace on string field values.
---

`com.kmwllc.lucille.stage.ReplacePatterns`

Performs pattern-based find-and-replace on string field values.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fields` | List\<String\> | Yes | Fields to process. |
| `patterns` | Map\<String, String\> | Yes | Map of regex pattern → replacement string. |
