---
title: Print
weight: 480
date: 2025-06-09
description: Logs documents in JSON format at INFO level and/or writes them to a file.
---

`com.kmwllc.lucille.stage.Print`

Logs documents in JSON format at INFO level and/or writes them to a file. Place `Print` anywhere in the pipeline to capture the document state at that point.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `shouldLog` | Boolean | No | Log each document as JSON at INFO level. Default: `true`. |
| `outputFile` | String | No | Path to a file to write documents to (one JSON object per line). Created if it does not exist. The file is opened when the first document arrives. |
| `whitelist` | List\<String\> | No | If set, only these fields are included in the output. The document passed to the next stage is not changed. |
| `blacklist` | List\<String\> | No | Fields to exclude from the output. The document passed to the next stage is not changed. |
| `overwriteFile` | Boolean | No | Overwrite the output file if it already exists. If `false`, documents are appended to it. Default: `true`. |
| `appendThreadName` | Boolean | No | Append the Worker thread name to the output filename, before the extension (e.g. `out.jsonl` becomes `out<threadName>.jsonl`), keeping per-thread output separate. Recommended when using multiple worker threads. Has no effect without `outputFile`. Default: `true`. |

```hocon
{
  class: "com.kmwllc.lucille.stage.Print"
  shouldLog: false
  outputFile: "/tmp/pipeline-output.jsonl"
}
```

#### Capture and Replay

`Print` enables a useful development pattern: run a pipeline to capture its output to disk, then replay that output directly into a search backend without re-running the enrichment.

**Step 1: Capture.** Add `Print` to the end of your pipeline with an `outputFile`. Use a `NopIndexer` (or set `sendEnabled: false` on a real indexer) so no data is actually indexed during the capture run:

```hocon
pipelines: [{
  name: my-pipeline
  stages: [
    # ... your enrichment stages ...
    {
      class: "com.kmwllc.lucille.stage.Print"
      shouldLog: false
      outputFile: "/tmp/captured.jsonl"
      appendThreadName: false
    }
  ]
}]
indexer { class: "com.kmwllc.lucille.indexer.NopIndexer" }
```

`appendThreadName: false` writes a single `/tmp/captured.jsonl`. With more than one worker thread, keep `appendThreadName` enabled instead (threads writing one file can corrupt it) and list each per-thread file in the replay connector's `paths`.

**Step 2: Replay.** Point a `FileConnector` at the captured JSONL file using the JSON file handler. Use a minimal or empty pipeline, since the captured documents already contain all enriched fields:

```hocon
connectors: [{
  name: replay
  class: "com.kmwllc.lucille.connector.FileConnector"
  pipeline: replay-pipeline
  paths: ["/tmp/captured.jsonl"]
  fileHandlers: { json: { idField: "id" } }
}]
pipelines: [{
  name: replay-pipeline
  stages: []
}]
indexer { type: OpenSearch }
opensearch { ... }
```

This lets you iterate on indexer configuration, field mappings, or search backend settings without repeating expensive enrichment (OCR, embedding generation, database lookups) on every attempt.
