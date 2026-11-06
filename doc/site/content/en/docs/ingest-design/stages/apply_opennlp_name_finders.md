---
title: ApplyOpenNLPNameFinders
weight: 120
date: 2025-06-09
description: Performs named entity recognition using Apache OpenNLP name finder models.
---

`com.kmwllc.lucille.entityextraction.stage.ApplyOpenNLPNameFinders` *(requires `lucille-entity-extraction`)*

Performs named entity recognition (NER) on `textField` using [Apache OpenNLP](https://opennlp.apache.org/) models. The text is split into tokens with the tokenizer model, then each name finder model in `models` is run over the tokens. The distinct names each model finds are added to the field named by that model's key in `models`. Only the first value of `textField` is used. Documents where `textField` is missing or blank are passed through unchanged. Adaptive data is cleared from each name finder after every document, so context from one document does not affect the next.

The tokenizer and name finder models are loaded at startup; the stage fails at startup if any of them cannot be loaded. Model paths can be local file paths or `classpath:` paths.

**Maven dependency:**
```xml
<dependency>
  <groupId>com.kmwllc</groupId>
  <artifactId>lucille-entity-extraction</artifactId>
  <version>${lucille.version}</version>
</dependency>
```

| Parameter | Type | Required | Description |
|---|---|---|---|
| `textField` | String | Yes | Field containing the text to search for names. |
| `tokenizerPath` | String | Yes | Path to an OpenNLP tokenizer model file. |
| `models` | Map\<String, String\> | Yes | Maps a destination field name to the path of an OpenNLP name finder model file. Names found by each model are added to its field. |

```hocon
{
  class: "com.kmwllc.lucille.entityextraction.stage.ApplyOpenNLPNameFinders"
  textField: "body"
  tokenizerPath: "/models/en-token.bin"
  models: {
    person: "/models/en-ner-person.bin"
    location: "/models/en-ner-location.bin"
  }
}
```
