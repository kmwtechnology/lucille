---
title: XPathExtractor
weight: 650
date: 2025-06-09
description: Evaluates XPath expressions against an XML field.
---

`com.kmwllc.lucille.stage.XPathExtractor`

Evaluates XPath expressions against an XML field.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `xmlField` | String | Yes | Field containing XML content. |
| `fieldMappings` | Map\<String, String\> | Yes | Map of destination field → XPath expression. |
