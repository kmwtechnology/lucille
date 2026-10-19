---
title: ExtractEntities
weight: 320
date: 2025-06-09
description: Finds dictionary terms in text fields using an Aho-Corasick trie and writes the matches or their payloads to destination fields.
---

`com.kmwllc.lucille.stage.ExtractEntities`

Finds occurrences of dictionary terms anywhere inside the values of each `source` field, using an Aho-Corasick trie, and writes the payloads of the matched terms to the corresponding `dest` field. See also [ExtractEntitiesFST]({{< relref "docs/ingest-design/stages/extract_entities_fst" >}}), which does the same with a Lucene FST, and [DictionaryLookup]({{< relref "docs/ingest-design/stages/dictionary_lookup" >}}), which matches whole field values.

Dictionaries are CSV files read as UTF-8. Each row is `term` or `term, payload`; columns after the second are ignored. A row with no payload uses the term as its payload. Terms and payloads are trimmed. Rows containing malformed characters are skipped with a warning. All dictionaries are loaded at startup, and a dictionary that cannot be read fails the stage at startup.

`source` and `dest` must have the same number of fields (1-1 mapping), or `dest` must have one field that receives the results from every `source` field. With a single `dest` field, each `source` field's results are written in turn under `updateMode`, so use `append` to keep the results from all of them. All values of a multi-valued source field are scanned. Source fields missing from the Document, and fields with no matches, are skipped.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | List\<String\> | Yes | Fields to scan for dictionary terms. |
| `dest` | List\<String\> | Yes | Destination fields. Same length as `source`, or a single field. |
| `dictionaries` | List\<String\> | Yes | Dictionary file paths. Each can be a local path, a `classpath:` path, or a cloud storage URI (`s3://`, `gs://`, or `https://<account>.blob.core.windows.net/...`) read with the `s3`, `gcp`, or `azure` credentials. |
| `usePayloads` | Boolean | No | Write payloads to `dest`. If `false`, writes the matched dictionary terms instead. Default: `true`. |
| `entityField` | String | No | If set and `usePayloads` is `true`, the matched dictionary terms are also written to this field. |
| `ignoreCase` | Boolean | No | Match case-insensitively. Default: `false`. |
| `onlyWholeWords` | Boolean | No | Matches must not be embedded in other letters. For example, `OMAN` does not match inside `rOMAN`. Default: `true`. |
| `onlyWhitespaceSeparated` | Boolean | No | Matches must be bounded by whitespace or the ends of the value. Default: `false`. |
| `ignoreOverlaps` | Boolean | No | Drop matches that overlap another match. Default: `false`. |
| `stopOnHit` | Boolean | No | Stop scanning a value after the first match. Default: `false`. |
| `updateMode` | String | No | `overwrite`, `append`, or `skip`. Applies to `dest` and `entityField`. Default: `overwrite`. |
| `s3` | Object | No | S3 credentials for reading dictionaries from S3: `accessKeyId`, `secretAccessKey`, `region`, `anonymous`, `maxResultsPerPage`. See [Cloud Storage Configuration]({{< relref "docs/ingest-design/connectors/file_connector#cloud-storage-configuration" >}}). |
| `azure` | Object | No | Azure credentials for reading dictionaries from Azure: `connectionString`, `accountName`, `accountKey`, `maxResultsPerPage`. See [Cloud Storage Configuration]({{< relref "docs/ingest-design/connectors/file_connector#cloud-storage-configuration" >}}). |
| `gcp` | Object | No | Google Cloud credentials for reading dictionaries from GCS: `pathToServiceKey` (required within `gcp`), `maxResultsPerPage`. See [Cloud Storage Configuration]({{< relref "docs/ingest-design/connectors/file_connector#cloud-storage-configuration" >}}). |

```hocon
{
  class: "com.kmwllc.lucille.stage.ExtractEntities"
  source: ["title", "body"]
  dest: ["countries"]
  dictionaries: ["classpath:dictionaries/countries.csv"]
  ignoreCase: true
  entityField: "country_mentions"
  updateMode: "append"
}
```
