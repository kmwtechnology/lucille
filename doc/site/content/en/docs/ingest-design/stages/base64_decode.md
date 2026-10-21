---
title: Base64Decode
weight: 140
date: 2025-06-09
description: Decodes a Base64-encoded field value into a byte array.
---

`com.kmwllc.lucille.stage.Base64Decode`

Decodes the Base64 string in `inputField` and writes the result to `outputField` as a byte array. Only the first value of a multi-valued field is decoded. Any existing value in `outputField` is replaced. Characters outside the Base64 alphabet are skipped rather than causing an error. Documents without `inputField` are left unchanged.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `inputField` | String | Yes | Field containing the Base64-encoded value. |
| `outputField` | String | Yes | Field to write the decoded bytes to. |
