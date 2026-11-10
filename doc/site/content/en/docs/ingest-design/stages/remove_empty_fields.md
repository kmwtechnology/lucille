---
title: RemoveEmptyFields
weight: 550
date: 2025-06-09
description: Removes fields whose value is null or an empty string.
---

`com.kmwllc.lucille.stage.RemoveEmptyFields`

Removes every field whose value is `null` or an empty string. All fields on the Document are checked. A multi-valued field is removed only if every value is null or empty, and a field holding an empty list is removed. Whitespace-only strings are not considered empty, and fields holding a nested object (even an empty one) are kept. Reserved fields, such as `id` and attached children, are never removed.

This stage has no parameters beyond the shared stage parameters.

```hocon
{ class: "com.kmwllc.lucille.stage.RemoveEmptyFields" }
```
