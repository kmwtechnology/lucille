---
title: Solr Connector
date: 2025-06-09
description: A Connector that queries Solr and publishes each result document into a Lucille pipeline. Supports pre/post actions for setup and cleanup.
---

[Source Code](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/connector/SolrConnector.java)

The `SolrConnector` issues a query against a Solr collection and publishes each result as a Lucille Document. It is useful for cross-index enrichment pipelines — for example, reading documents from one Solr collection, enriching them, and indexing them into a different collection or backend.

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
| `solrParams` | Map\<String, Object\> | No | Solr query parameters passed directly to the query. Used when `pipeline` is configured. |
| `preActions` | List\<String\> | No | Solr update requests to send before executing the main query (see below). |
| `postActions` | List\<String\> | No | Solr update requests to send after executing the main query (see below). |
| `useXml` | Boolean | No | If `true`, sends action requests as XML instead of JSON. Default: `false`. |
| `idField` | String | No | Solr field to use as the Lucille Document ID. Default: `id`. |

### `solr` Connection Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `url` | List\<String\> | Yes | Solr URL(s). For basic mode, include the collection (e.g., `http://localhost:8983/solr/my-collection`). For cloud mode, omit the collection. |
| `useCloudClient` | Boolean | No | Use `CloudHttp2SolrClient` (SolrCloud). Default: `false`. |
| `defaultCollection` | String | No | Default collection name for cloud mode. |
| `zkHosts` | List\<String\> | No | ZooKeeper hosts for SolrCloud connection (alternative to `url` in cloud mode). |
| `zkChroot` | String | No | ZooKeeper chroot path (e.g., `/solr`). |
| `userName` | String | No | Basic auth username. |
| `password` | String | No | Basic auth password. |
| `acceptInvalidCert` | Boolean | No | Accept self-signed or invalid SSL certificates. |

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
- `q` — query string (default: `*:*` if omitted)
- `fl` — field list (fields to return; omit for all fields)
- `fq` — filter query (can be a single string or a list for multiple filters)
- `rows` — page size for cursor pagination (default: Solr's default, typically 10)

The connector always adds `sort: {idField} asc` automatically (required for cursor pagination) and manages `cursorMark` internally.

## Pre and Post Actions

`preActions` and `postActions` are lists of Solr update requests sent before and after the main query, respectively. They support the `{runId}` placeholder, which is replaced with the current run's UUID at execution time.

The request format is controlled by `useXml`:
- `useXml: false` (default): requests are JSON strings
- `useXml: true`: requests are XML strings

**Example: Mark documents as in-progress before reading, mark as done after**

```hocon
connectors: [
  {
    name: "solr-source"
    class: "com.kmwllc.lucille.connector.SolrConnector"
    pipeline: "process-pipeline"

    solr {
      url: ["http://localhost:8983/solr/my-collection"]
    }

    # JSON update — set run_id on all active documents before reading
    preActions: [
      "{\"add\":{\"doc\":{\"id\":\"marker\",\"run_id\":\"{runId}\"}}}"
    ]

    # JSON update — clean up after run
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

`postActions` run only if `preActions` and the main query both succeed. If the connector throws, `postActions` are skipped.

## Cursor-Based Pagination

The connector iterates through results using Solr's `cursorMark` mechanism, which avoids deep pagination performance issues. This requires:

1. Documents to be sorted by a unique field (the `idField`).
2. The `idField` to be indexed in Solr as a single-valued, non-analyzed field.

For large collections, tune `rows` in `solrParams` to balance memory use and round-trip count.
