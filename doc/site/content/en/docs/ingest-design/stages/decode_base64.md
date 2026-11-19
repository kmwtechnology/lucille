---
title: DecodeBase64
weight: 250
aliases:
  - /docs/ingest-design/stages/base64_decode/
date: 2025-06-09
description: Decodes a Base64-encoded field value into a byte array.
---

`com.kmwllc.lucille.stage.DecodeBase64`

Decodes the Base64 string in `source` and writes the result to `dest` as a byte array. Only the first value of a multi-valued field is decoded. Any existing value in `dest` is replaced. Characters outside the Base64 alphabet are skipped rather than causing an error. Documents without `source` are left unchanged.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | String | Yes | Field containing the Base64-encoded value. |
| `dest` | String | Yes | Field to write the decoded bytes to. |
