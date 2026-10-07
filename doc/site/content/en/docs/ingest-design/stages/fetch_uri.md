---
title: FetchUri
weight: 360
date: 2025-06-09
description: Fetches the content of a URL stored in a document field and stores the response as a byte array.
---

`com.kmwllc.lucille.stage.FetchUri`

Fetches the content of a URL stored in a document field and stores the response as a byte array.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | String | Yes | Field containing the URL to fetch. |
| `dest` | String | Yes | Field to write the response bytes to. |
