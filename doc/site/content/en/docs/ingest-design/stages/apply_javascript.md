---
title: ApplyJavascript
weight: 80
date: 2025-06-09
description: Runs a JavaScript snippet per document using GraalVM's JavaScript engine.
---

`com.kmwllc.lucille.stage.ApplyJavascript`

Runs a JavaScript snippet per document using GraalVM's JavaScript engine.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `script` | String | No | Inline JavaScript code. |
| `scriptPath` | String | No | Path to a `.js` file. Exactly one of `script` or `scriptPath` must be provided. |
