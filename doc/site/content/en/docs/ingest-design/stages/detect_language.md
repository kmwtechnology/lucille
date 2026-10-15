---
title: DetectLanguage
weight: 240
date: 2025-06-09
description: Detects the language of a text field and writes the ISO language code to a destination field.
---

`com.kmwllc.lucille.stage.DetectLanguage`

Detects the language of a text field and writes the ISO language code to a destination field.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | String | Yes | Field containing the text. |
| `dest` | String | Yes | Destination field for the language code (e.g., `"en"`, `"fr"`). |
