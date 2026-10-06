---
title: ApplyFileHandlers
weight: 70
date: 2025-06-09
description: Applies configured FileHandlers to a byte array field, generating child documents for each extracted record.
---

`com.kmwllc.lucille.stage.ApplyFileHandlers`

Applies configured FileHandlers to a byte array field, generating child documents for each extracted record.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fileContentField` | String | No | Field containing the file bytes. Default: `file_content`. |
| `filePathField` | String | No | Field containing the file path (used to determine handler). Default: `file_path`. |
| `fileHandlers` | Object | **Yes** | FileHandler configuration — same structure as the `fileHandlers` block in `FileConnector`. At least one handler must be declared. |
