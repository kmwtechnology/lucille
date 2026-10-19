---
title: ExtractEntitiesFST
weight: 330
date: 2025-06-09
description: Finds dictionary terms in text fields using a Lucene FST and writes the matches or their payloads to destination fields.
---

`com.kmwllc.lucille.stage.ExtractEntitiesFST`

Finds occurrences of dictionary terms anywhere inside the values of each `source` field, using a Lucene finite-state transducer (FST), and writes the payloads of the matched terms to the corresponding `dest` field. It is an alternative to [ExtractEntities]({{< relref "docs/ingest-design/stages/extract_entities" >}}) with the same configuration, minus `onlyWhitespaceSeparated` and plus `dictsSorted`.

Dictionaries are CSV files read as UTF-8. Each row is `term` or `term, payload`; columns after the second are ignored. A row with no payload uses the term as its payload. Terms and payloads are trimmed, and terms are lowercased when `ignoreCase` is `true`. If a term appears more than once, across all dictionaries, the first row wins. Rows containing malformed characters are skipped with a warning. All dictionaries are loaded at startup, and a dictionary that cannot be read fails the stage at startup.

`source` and `dest` must have the same number of fields (1-1 mapping), or `dest` must have one field that receives the results from every `source` field. With a single `dest` field, each `source` field's results are written in turn under `updateMode`, so use `append` to keep the results from all of them. All values of a multi-valued source field are scanned. Source fields missing from the Document, and fields with no matches, are skipped.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | List\<String\> | Yes | Fields to scan for dictionary terms. |
| `dest` | List\<String\> | Yes | Destination fields. Same length as `source`, or a single field. |
| `dictionaries` | List\<String\> | Yes | Dictionary file paths. Each can be a local path, a `classpath:` path, or a cloud storage URI (`s3://`, `gs://`, or `https://<account>.blob.core.windows.net/...`) read with the `s3`, `gcp`, or `azure` credentials. |
| `usePayloads` | Boolean | No | Write payloads to `dest`. If `false`, writes the matched terms instead. Default: `true`. |
| `entityField` | String | No | If set and `usePayloads` is `true`, the matched terms are also written to this field. Terms are lowercased when `ignoreCase` is `true`. |
| `ignoreCase` | Boolean | No | Match case-insensitively. Default: `false`. |
| `onlyWholeWords` | Boolean | No | Matches must not be preceded or followed by a letter. Default: `true`. |
| `ignoreOverlaps` | Boolean | No | Keep only the longest match starting at each position and continue scanning after it. If `false`, all matches starting at each position are kept, including overlapping ones. Default: `false`. |
| `stopOnHit` | Boolean | No | Stop scanning a field after the first position where a match starts. Default: `false`. |
| `dictsSorted` | Boolean | No | Skip sorting dictionary terms at startup, to reduce startup time. Set only if the terms are already sorted (after trimming, and lowercasing if `ignoreCase` is `true`). Default: `false`. |
| `updateMode` | String | No | `overwrite`, `append`, or `skip`. Applies to `dest` and `entityField`. Default: `overwrite`. |
| `s3` | Object | No | S3 credentials for reading dictionaries from S3: `accessKeyId`, `secretAccessKey`, `region`, `anonymous`, `maxResultsPerPage`. See [Cloud Storage Configuration]({{< relref "docs/ingest-design/connectors/file_connector#cloud-storage-configuration" >}}). |
| `azure` | Object | No | Azure credentials for reading dictionaries from Azure: `connectionString`, `accountName`, `accountKey`, `maxResultsPerPage`. See [Cloud Storage Configuration]({{< relref "docs/ingest-design/connectors/file_connector#cloud-storage-configuration" >}}). |
| `gcp` | Object | No | Google Cloud credentials for reading dictionaries from GCS: `pathToServiceKey` (required within `gcp`), `maxResultsPerPage`. See [Cloud Storage Configuration]({{< relref "docs/ingest-design/connectors/file_connector#cloud-storage-configuration" >}}). |

```hocon
{
  class: "com.kmwllc.lucille.stage.ExtractEntitiesFST"
  source: ["body"]
  dest: ["companies"]
  dictionaries: ["/data/dictionaries/companies.csv"]
  ignoreCase: true
  ignoreOverlaps: true
}
```
