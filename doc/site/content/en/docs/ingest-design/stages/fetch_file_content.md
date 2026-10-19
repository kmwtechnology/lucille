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
| `s3` / `azure` / `gcp` | Map | No | Cloud storage credentials, with the same options as `FileConnector`. Required to fetch from that storage system; local paths need none. |

Use this stage with `FileConnector`'s `fileOptions.getFileContent: false` to keep file content off the source topic and read it in the Worker instead. Delete `file_content` with `DeleteFields` once the stages that need it have run. See [Content Crawls]({{< relref "docs/operations/performance-tuning#content-crawls-publish-the-path-fetch-content-in-the-worker" >}}).
