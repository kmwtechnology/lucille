---
title: XPathExtractor
weight: 650
date: 2025-06-09
description: Extracts the text of nodes matched by XPath expressions from an XML field.
---

`com.kmwllc.lucille.stage.XPathExtractor`

Parses the XML in `xmlField` and evaluates each XPath expression in `fieldMapping`. The text content of every matched node is added to each of the expression's destination fields.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `xmlField` | String | No | Field containing the XML. If the field is multi-valued, only the first value is used. Default: `xml`. |
| `fieldMapping` | Map\<String, List\<String\>\> | Yes | Map of XPath expression to the list of destination fields. Quote the expressions, since in an unquoted HOCON key `//` starts a comment and `.` is a path separator. An expression that does not compile fails at startup. |

## Behavior

- Documents without `xmlField` are not changed.
- Matched values are added to the destination fields; existing values are kept.
- Expressions must select nodes. If the XML cannot be parsed or an expression does not return nodes, an error is logged and the document continues without the remaining values.

```hocon
{
  class: "com.kmwllc.lucille.stage.XPathExtractor"
  xmlField: "xml"
  fieldMapping: {
    "/book/title": ["title"]
    "//author/name": ["authors", "all_names"]
  }
}
```
