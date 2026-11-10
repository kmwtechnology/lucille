---
title: ExtractFirstCharacter
weight: 340
date: 2025-06-09
description: Extracts the first character of fields, with a replacement value when it is not a letter.
---

`com.kmwllc.lucille.stage.ExtractFirstCharacter`

For each source field in `fieldMapping`, writes the first character of the field's value to the destination field, overwriting it. If the first character is not a letter (including digits), `replacement` is written instead. Only the first value of a multi-valued field is used. Missing or blank fields are skipped. The value is not trimmed, so a value starting with whitespace gets `replacement`.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fieldMapping` | Map\<String, String\> | Yes | Maps each source field to its destination field. Must not be empty. |
| `replacement` | String | No | Value written when the first character is not a letter. Use `SKIP` to write nothing instead. Default: `nonalpha`. |
