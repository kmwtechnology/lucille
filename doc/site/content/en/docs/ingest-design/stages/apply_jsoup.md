---
title: ApplyJSoup
weight: 100
date: 2025-06-09
description: Parses HTML content using JSoup and extracts text or attribute values using CSS selectors.
---

`com.kmwllc.lucille.stage.ApplyJSoup`

Parses HTML content using [JSoup](https://jsoup.org/) and extracts text or attribute values using CSS selectors.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `byteArrayField` | String | Yes | Field containing the HTML as a byte array. |
| `destinationFields` | Map | Yes | Map of destination field name → `{type, selector}` definition. |

Each destination field definition requires:
- `type`: `"text"` (inner text) or `"attr"` (attribute value).
- `selector`: CSS selector.
- `attr`: (if `type` is `"attr"`) the attribute name to extract.

```hocon
{
  class: "com.kmwllc.lucille.stage.ApplyJSoup"
  byteArrayField: "html_content"
  destinationFields: {
    title: { type: "text", selector: "h1" }
    body:  { type: "text", selector: ".article-body p" }
  }
}
```
