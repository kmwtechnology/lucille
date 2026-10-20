---
title: ApplyJSoup
weight: 100
date: 2025-06-09
description: Parses HTML using JSoup and extracts text, attribute values, or HTML using CSS selectors.
---

`com.kmwllc.lucille.stage.ApplyJSoup`

Parses HTML using [JSoup](https://jsoup.org/) and extracts text, attribute values, or HTML from the elements matched by [CSS selectors](https://jsoup.org/cookbook/extracting-data/selector-syntax). The HTML is read from one of three sources: a file path, a string, or a byte array held in a document field.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `filePathField` | String | No | Field containing a local path to an HTML file. |
| `stringField` | String | No | Field containing the HTML as a string. |
| `byteArrayField` | String | No | Field containing the HTML as a byte array. |
| `charset` | String | No | Charset of the HTML. With `filePathField`, if not set, the charset is detected from the byte-order mark or meta tags, falling back to UTF-8. With `byteArrayField`, if not set, UTF-8 is used. Ignored with `stringField`. |
| `destinationFields` | Map | Yes | Map of destination field name to a selector map. See below. |

Exactly one of `filePathField`, `stringField`, or `byteArrayField` must be set, or the stage fails at startup.

Each selector map in `destinationFields` has:
- `selector`: CSS selector. Required.
- `type`: `text` (element text), `attribute` (attribute value), `html` (inner HTML), or `outerHtml` (outer HTML). Required. Other values are not rejected and extract nothing.
- `attribute`: the attribute name to extract. Required when `type` is `attribute`.

Missing `selector` or `type`, or a missing `attribute` for `attribute` extraction, fails at startup.

## Behavior

- Documents without the configured source field are not modified.
- Destination fields are overwritten. If a selector matches several elements, the destination gets one value per element (for `text` and `attribute`, elements with no text or without the attribute are skipped).
- If a selector matches nothing, the destination field is left unchanged.

```hocon
{
  class: "com.kmwllc.lucille.stage.ApplyJSoup"
  byteArrayField: "html_content"
  destinationFields: {
    title: { type: "text", selector: "h1" }
    body:  { type: "text", selector: ".article-body p" }
    links: { type: "attribute", selector: "a", attribute: "href" }
  }
}
```
