---
title: DictionaryLookup
weight: 250
date: 2025-06-09
description: Looks up whole field values in a dictionary and writes their payloads, or a true/false membership flag, to destination fields.
---

`com.kmwllc.lucille.stage.DictionaryLookup`

Looks up each value of each `source` field in a dictionary. A value matches only if the whole value equals a dictionary term (to find terms inside longer text, use [ExtractEntities]({{< relref "docs/ingest-design/stages/extract_entities" >}})). The payloads of matching values are written to the corresponding `dest` field. Source fields missing from the Document, and fields with no matches, are skipped.

The dictionary is a CSV file. Each row is `term` or `term, payload1, payload2, ...`; all payload columns are used. A row with no payload uses the term as its payload. Terms and payloads are trimmed. Rows containing malformed characters are skipped with a warning. The dictionary is loaded at startup and shared by all stage instances that use the same `dictPath`, `ignoreCase`, and `setOnly`. A dictionary that cannot be read fails the stage at startup.

`source` and `dest` must have the same number of fields (1-1 mapping), or `dest` must have one field that receives the results from every `source` field.

With `setOnly: true`, the stage writes a Boolean to `dest` instead of payloads: `true` if every value of the source field is in the dictionary, `false` otherwise. With a single `dest` and several `source` fields, `dest` is `true` only if this holds for every source field. Without `useAnyMatch`, `dest` is always written in this mode, including `false` when a source field is missing (unless `ignoreMissingSource` is `true`). In this mode `updateMode` must be `overwrite` and dictionary rows must not have payloads; otherwise the stage fails at startup.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | List\<String\> | Yes | Fields whose values are looked up. |
| `dest` | List\<String\> | Yes | Destination fields. Same length as `source`, or a single field. Outside `setOnly` mode, with a single field, each source field's result is written to it in turn under `updateMode`, so use `append` to keep the results from every source. |
| `dictPath` | String | Yes | Dictionary file path. Can be a local path, a `classpath:` path, or a cloud storage URI (`s3://`, `gs://`, or `https://<account>.blob.core.windows.net/...`) read with the `s3`, `gcp`, or `azure` credentials. |
| `usePayloads` | Boolean | No | Write payloads to `dest`. If `false`, writes the matching source values instead. Default: `true`. |
| `ignoreCase` | Boolean | No | Match case-insensitively. Dictionary terms and source values are lowercased, so with `usePayloads: false` the lowercased values are written. Default: `false`. |
| `setOnly` | Boolean | No | Write a `true`/`false` membership flag to `dest` instead of payloads. See above. Default: `false`. |
| `useAnyMatch` | Boolean | No | Only with `setOnly`. Set `dest` to `true` as soon as any value of any source field is in the dictionary, instead of requiring all values. The first match ends processing of the Document, so with several `dest` fields, those of later `source` fields are not written. Default: `false`. |
| `ignoreMissingSource` | Boolean | No | Only with `setOnly`. Treat a missing source field as a match (`true`) instead of `false`. Default: `false`. |
| `updateMode` | String | No | `overwrite`, `append`, or `skip`. Must be `overwrite` when `setOnly` is `true`. Default: `overwrite`. |
| `s3` | Object | No | S3 credentials for reading the dictionary from S3: `accessKeyId`, `secretAccessKey`, `region`, `anonymous`, `maxNumOfPages`. See [Cloud Storage Configuration]({{< relref "docs/ingest-design/connectors/file_connector#cloud-storage-configuration" >}}). |
| `azure` | Object | No | Azure credentials for reading the dictionary from Azure: `connectionString`, `accountName`, `accountKey`, `maxNumOfPages`. See [Cloud Storage Configuration]({{< relref "docs/ingest-design/connectors/file_connector#cloud-storage-configuration" >}}). |
| `gcp` | Object | No | Google Cloud credentials for reading the dictionary from GCS: `pathToServiceKey` (required within `gcp`), `maxNumOfPages`. See [Cloud Storage Configuration]({{< relref "docs/ingest-design/connectors/file_connector#cloud-storage-configuration" >}}). |

```hocon
{
  class: "com.kmwllc.lucille.stage.DictionaryLookup"
  source: ["country_code"]
  dest: ["country_name"]
  dictPath: "classpath:dictionaries/country_codes.csv"
  ignoreCase: true
}
```
