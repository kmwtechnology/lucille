---
title: ParseFilePath
weight: 450
date: 2025-06-09
description: Extracts path components (directory, filename, extension) from a file path field.
---

`com.kmwllc.lucille.stage.ParseFilePath`

Extracts path components (directory, filename, extension) from a file path field.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | String | Yes | Field containing the file path. |
| `directoryDest` | String | No | Destination for the directory component. |
| `filenameDest` | String | No | Destination for the filename (without extension). |
| `extensionDest` | String | No | Destination for the file extension. |
