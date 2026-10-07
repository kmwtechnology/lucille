---
title: FetchFileContent
weight: 350
date: 2025-06-09
description: Loads the content of a file path into a byte array field on the document.
---

`com.kmwllc.lucille.stage.FetchFileContent`

Loads the content of a file path (from local filesystem or cloud storage) into a byte array field on the document.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `filePathField` | String | No | Field containing the file path or URI. Default: `file_path`. |
| `fileContentField` | String | No | Destination byte array field. Default: `file_content`. |
| `maxSizeBytes` | Long | No | Maximum file size to load; must be positive and at most 2147483638. Larger files cause a StageException. Default: unlimited. |
