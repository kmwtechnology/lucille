---
title: RemoveEmptyFields
weight: 550
date: 2025-06-09
description: Removes fields whose value is null or an empty string.
---

`com.kmwllc.lucille.stage.RemoveEmptyFields`

Removes every field whose value is `null` or an empty string. All fields on the Document are checked. For a multi-valued field, only the first value is checked: if it is null or empty, the whole field is removed. Whitespace-only strings are not considered empty. Fields holding a nested object are also removed.

Processing fails with an exception for a Document that has a field holding an empty list, or that has attached children.

This stage has no parameters beyond the shared stage parameters.

```hocon
{ class: "com.kmwllc.lucille.stage.RemoveEmptyFields" }
```
