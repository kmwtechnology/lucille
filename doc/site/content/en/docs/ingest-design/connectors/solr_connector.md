---
title: Solr Connector
weight: 3
date: 2025-06-09
description: A Connector that queries Solr and publishes each result document into a Lucille pipeline. Supports pre/post actions for setup and cleanup.
---

[Source Code](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/SolrConnector.java)

`com.kmwllc.lucille.connector.SolrConnector`

The `SolrConnector` sends optional update requests to Solr before and after a run (`preActions` and `postActions`). When the connector has a `pipeline`, it also queries Solr and publishes each result as a Lucille Document. It is useful for cross-index enrichment pipelines, for example reading documents from one Solr collection, enriching them, and indexing them into a different collection or backend. Without a `pipeline`, no query is run and only the actions are sent.

Internally, the connector uses cursor-based pagination (`cursorMark`) to iterate through large result sets without loading the full result into memory. Results are sorted by `idField` ascending (required for cursor pagination).

## Basic Configuration

```hocon
connectors: [
  {
    name: "solr-source"
    class: "com.kmwllc.lucille.connector.SolrConnector"
    pipeline: "my-pipeline"

    solr {
      url: ["http://localhost:8983/solr"]
      useCloudClient: true
      defaultCollection: "source-collection"
    }

    solrParams {
      q: "*:*"
      fl: "id,title,body,author"
      fq: "status:active"
      rows: 100
    }

    idField: "id"
  }
]
```

## Configuration Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `solr` | Object | Yes | Solr connection parameters (see below). |
| `solrParams` | Map\<String, Object\> | No | Solr query parameters, used when `pipeline` is set and ignored otherwise. If omitted, the query carries only the connector's own `sort` and `cursorMark`. String, number, and list-of-string values are passed to the query; a list containing non-strings fails when the query is built, and other value types (such as booleans) are ignored. |
| `idField` | String | No | Solr field whose value becomes the Lucille Document ID, and the cursor sort field, so it must be the collection's `uniqueKey` field. When there is no `pipeline` it is not used. Default: `id`. |
| `preActions` | List\<String\> | No | Solr update requests to send before the query (see below). |
| `postActions` | List\<String\> | No | Solr update requests to send after the query (see below). |
| `useXml` | Boolean | No | If `true`, sends action requests as XML instead of JSON. Default: `false`. |

### `solr` Connection Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `url` | String or List\<String\> | No | Solr URL(s). Required unless `useCloudClient` is `true` and `zkHosts` is set; omitting it otherwise fails at startup. Used to build the SolrCloud client when `useCloudClient` is `true` and `zkHosts` is not set. It is not passed to the standalone client built when `useCloudClient` is `false`, which then has no base URL; use `useCloudClient: true` with this connector. |
| `useCloudClient` | Boolean | No | Use `CloudHttp2SolrClient` (SolrCloud). Set it to `true` for this connector; see `url`. Default: `false`. |
| `defaultCollection` | String | No | Default collection for the SolrCloud client. The query and all actions are sent to this collection, so set it. |
| `zkHosts` | List\<String\> | No | ZooKeeper hosts for the SolrCloud client. When set, used instead of `url` to connect. |
| `zkChroot` | String | No | ZooKeeper chroot path (e.g., `/solr`). Only used with `zkHosts`. |
| `userName` | String | No | Basic auth username. Must be set together with `password`; if only one is set, an error is logged and no authentication is used. |
| `password` | String | No | Basic auth password. |
| `acceptInvalidCert` | Boolean | No | If `true`, sets the `solr.ssl.checkPeerName` system property to `false`. The `HttpJdkSolrClient` that Lucille builds (also used under the SolrCloud client) does not read that property, so this setting has no effect. Default: `false`. |
| `javax.net.ssl.keyStore` | String | No | Sets the JVM system property of the same name when the client is created, unless it is already set. The same key at the top level of the connector config is also read and takes precedence, but the connector's Spec rejects it there, so set it in `solr`. |
| `javax.net.ssl.keyStorePassword` | String | No | Same as `javax.net.ssl.keyStore`. |
| `javax.net.ssl.trustStore` | String | No | Same as `javax.net.ssl.keyStore`. |
| `javax.net.ssl.trustStorePassword` | String | No | Same as `javax.net.ssl.keyStore`. |

## Document Fields

Each Solr result becomes a Document whose ID is `docIdPrefix` (if set) followed by the value of `idField`. Every other returned field is copied to the Document, with its name lowercased. A result whose `idField` value is missing or blank is skipped and logged, and the number of skipped results is logged at the end. The `idField` and `id` fields are not copied. Field values are copied as strings, so `idField` and every returned field must hold string values; a field with a non-string value (for example a number or date), or a field whose name contains uppercase letters, fails the run. Solr also returns its numeric `_version_` field unless `fl` excludes it, so always set `fl` to the string fields you need. Nested child documents are not read.

## Query Parameters (`solrParams`)

The `solrParams` block accepts any standard Solr query parameters:

```hocon
solrParams {
  q: "status:active AND type:article"
  fl: "id,title,body,published_date"
  fq: ["category:news", "language:en"]
  rows: 500
}
```

Common parameters:
- `q`: query string
- `fl`: field list (fields to return; omit for all fields)
- `fq`: filter query (can be a single string or a list for multiple filters)
- `rows`: page size for cursor pagination (if omitted, Solr's default is used)

Do not set `sort` or `cursorMark`: `cursorMark` is overwritten, and a `sort` value is sent in addition to the connector's own, which can break cursor pagination.

The connector always adds `sort={idField} asc` (required for cursor pagination) and manages `cursorMark` internally.

## Pre and Post Actions

`preActions` and `postActions` are lists of Solr update requests sent before and after the main query, respectively. They support the `{runId}` placeholder, which is replaced with the current run's UUID at execution time.

The request format is controlled by `useXml`:
- `useXml: false` (default): requests are JSON strings
- `useXml: true`: requests are XML strings

**Example: Add a marker document before reading, delete it after**

```hocon
connectors: [
  {
    name: "solr-source"
    class: "com.kmwllc.lucille.connector.SolrConnector"
    pipeline: "process-pipeline"

    solr {
      url: ["http://localhost:8983/solr"]
      useCloudClient: true
      defaultCollection: "my-collection"
    }

    idField: "id"

    # JSON update: add a marker document carrying the run ID before reading
    preActions: [
      "{\"add\":{\"doc\":{\"id\":\"marker\",\"run_id\":\"{runId}\"}}}"
    ]

    # JSON update: delete documents carrying the run ID after the run
    postActions: [
      "{\"delete\":{\"query\":\"run_id:{runId}\"}}"
    ]

    solrParams {
      q: "status:active"
      fl: "id,title,body"
    }
  }
]
```

`preActions` are sent in the connector's `preExecute` step and `postActions` in `postExecute`. `postActions` run only if `preActions` and the main query both succeed. If an action request fails, the run fails.

## Cursor-Based Pagination

The connector iterates through results using Solr's `cursorMark` mechanism, which avoids deep pagination performance issues. This requires:

1. Documents to be sorted by the collection's `uniqueKey` field, so `idField` must be that field.
2. The `idField` to hold a single string value in each document.

For large collections, tune `rows` in `solrParams` to balance memory use and round-trip count.
