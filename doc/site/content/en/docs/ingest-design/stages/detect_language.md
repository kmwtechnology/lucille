---
title: DetectLanguage
weight: 240
date: 2025-06-09
description: Detects the language of text fields and writes the language code and confidence to fields.
---

`com.kmwllc.lucille.stage.DetectLanguage`

Detects the language of the combined text of the `source` fields and writes the language code (such as `en` or `fr`) to `languageField` and the probability, rounded down to two decimals, to `languageConfidenceField`. Nothing is written if the text is shorter than `minLength` or the best result is below `minProbability`. If detection fails, for example on text with no letters, the Document fails with a `StageException`.

On first use, the language profiles are copied to `DetectLanguage/profiles` under the Lucille home directory (the `LUCILLE_HOME` system property, or the working directory if it is not set). They are not copied again while that directory exists.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | List\<String\> | Yes | Fields containing the text. |
| `languageField` | String | Yes | Field for the detected language code. |
| `languageConfidenceField` | String | No | Field for the detection probability. Default: `languageConfidence`. |
| `minLength` | Integer | No | Minimum combined text length needed to run detection. Default: `50`. |
| `maxLength` | Integer | No | Maximum number of characters read, from the start of the combined text. Default: `10000`. |
| `minProbability` | Double | No | Minimum probability for a result to be written. Default: `0.95`. |
| `updateMode` | String | No | `overwrite`, `append`, or `skip`. Default: `overwrite`. |
