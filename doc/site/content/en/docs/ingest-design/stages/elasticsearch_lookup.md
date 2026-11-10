---
title: ElasticsearchLookup
weight: 280
date: 2025-06-09
description: Looks up a document in Elasticsearch by the Lucille document's ID and copies selected fields onto the document.
---

`com.kmwllc.lucille.elasticsearch.stage.ElasticsearchLookup`

**Maven dependency:**
```xml
<dependency>
  <groupId>com.kmwllc</groupId>
  <artifactId>lucille-elasticsearch</artifactId>
  <version>${lucille.version}</version>
</dependency>
```

Fetches the Elasticsearch document in `elasticsearch.index` whose ID matches the Lucille document's ID, then copies each field in `source` to the field at the same position in `dest`. `source` and `dest` must have the same length, or the stage fails at startup. If no matching Elasticsearch document exists, or a source field is missing from it or `null`, nothing is written for that field. Values keep their JSON types: numbers stay numbers, objects are copied as objects, and an array is written as a multi-valued field without its null elements. The stage pings Elasticsearch at startup and fails if the cluster is unreachable.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `elasticsearch.url` | String | Yes | Elasticsearch endpoint URL. Can also be a list of URLs for the same cluster. Basic auth credentials can be embedded in the URL (`https://user:password@host:9200`). |
| `elasticsearch.index` | String | Yes | Index to look documents up in. |
| `elasticsearch.acceptInvalidCert` | Boolean | No | Accept invalid or self-signed TLS certificates. Default: `false`. |
| `elasticsearch.useCompression` | Boolean | No | Enable compression in the Elasticsearch HTTP client. Default: `false`. |
| `elasticsearch.connectTimeoutMs` | Long | No | Maximum time to establish a connection, in ms. Default: `5000`. |
| `elasticsearch.socketTimeoutMs` | Long | No | Maximum time to wait for data on an open connection, including a response, in ms. Set `-1` for no limit. Default: `30000`. |
| `elasticsearch.connectionTimeToLiveMs` | Long | No | Maximum lifetime of a pooled connection, in ms, so long-running clients behind a load balancer or Kubernetes Service reconnect and reach new nodes. Default: `300000` (5 minutes). |
| `source` | List\<String\> | Yes | Fields to read from the Elasticsearch document. |
| `dest` | List\<String\> | Yes | Fields to write to, matched to `source` by position. |
| `updateMode` | String | No | `overwrite`, `append`, or `skip`. Default: `overwrite`. |

The `elasticsearch` block also accepts `update` and `join` because it is shared with the [Elasticsearch Indexer]({{< relref "docs/ingest-design/indexers/elasticsearch_indexer" >}}). This stage ignores them.

```hocon
{
  class: "com.kmwllc.lucille.elasticsearch.stage.ElasticsearchLookup"
  elasticsearch {
    url: "http://localhost:9200"
    index: "products"
  }
  source: ["price", "category"]
  dest: ["es_price", "es_category"]
}
```
