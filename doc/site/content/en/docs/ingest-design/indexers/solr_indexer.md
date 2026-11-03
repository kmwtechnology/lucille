---
title: Solr Indexer
weight: 1
date: 2025-06-09
description: Configuration reference for the Solr Indexer, for single-node Solr and SolrCloud.
---

`com.kmwllc.lucille.solr.indexer.SolrIndexer`

Config block: `solr { ... }`

**Maven dependency:**
```xml
<dependency>
  <groupId>com.kmwllc</groupId>
  <artifactId>lucille-solr</artifactId>
  <version>${lucille.version}</version>
</dependency>
```

Selecting the indexer with `indexer.type: "Solr"` works once `lucille-solr` is on the classpath.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `url` | String or List\<String\> | No | One or more Solr base URLs (e.g., `http://localhost:8983/solr`). Required unless `useCloudClient` is `true` and `zkHosts` is set: without it the indexer fails at startup (not checked when `indexer.sendEnabled` is `false`). Read only when `useCloudClient` is `true` and `zkHosts` is not set. |
| `useCloudClient` | Boolean | No | Use the SolrCloud client. Default: `false`. |
| `defaultCollection` | String | No | Collection that documents go to when no index override applies. Read only by the SolrCloud client. |
| `zkHosts` | List\<String\> | No | ZooKeeper addresses for SolrCloud (e.g., `["zk1:2181", "zk2:2181"]`). Takes precedence over `url` when `useCloudClient` is `true`. |
| `zkChroot` | String | No | ZooKeeper chroot path for SolrCloud (e.g., `"/solr"`). Used only with `zkHosts`. |
| `userName` | String | No | HTTP Basic Auth username. Used only when `password` is also set. If only one of the two is set, an error is logged and no authentication is used. |
| `password` | String | No | HTTP Basic Auth password. Used only when `userName` is also set. |
| `acceptInvalidCert` | Boolean | No | If `true`, sets the `solr.ssl.checkPeerName` system property to `false`. The `HttpJdkSolrClient` that Lucille builds (also used under the SolrCloud client) does not read that property, so this setting has no effect. Default: `false`. |

When `useCloudClient` is `false`, `url` must still be set, but the client is created without a base URL: `url` and `defaultCollection` are not used in that mode, and no other setting supplies a Solr address.

SolrIndexer supports `indexer.maxConcurrentBatches` greater than 1: concurrent batches share one Solr client.

```hocon
# SolrCloud with URL
solr {
  useCloudClient: true
  url: ["http://localhost:8983/solr"]
  defaultCollection: "my-collection"
}

# SolrCloud with ZooKeeper
solr {
  useCloudClient: true
  zkHosts: ["zk1:2181", "zk2:2181"]
  zkChroot: "/solr"
  defaultCollection: "my-collection"
}
```

---

## SSL

When the client is created, SolrIndexer copies the config keys `javax.net.ssl.keyStore`, `javax.net.ssl.keyStorePassword`, `javax.net.ssl.trustStore`, and `javax.net.ssl.trustStorePassword` into JVM system properties of the same name. A system property that is already set (for example with `-D`) is not overridden. These keys can be set at the root of the config or inside the `solr` block; if a key is set in both, the root value is used.

```hocon
javax.net.ssl.trustStore: "/path/to/truststore.jks"
javax.net.ssl.trustStorePassword: ${?TRUSTSTORE_PASSWORD}
```

---

## Child Document Support

When a document has children (via `doc.addChild()`), they are added to the Solr document as nested child documents. The `___children` field itself is never sent to Solr. Children of children are not sent.

Children respect the configured blacklist and whitelist: filtered fields are removed from child documents before they are added to the Solr parent. Children are sent with their own `id`; `indexer.idOverrideField` is applied only to the parent.

---

## Collection Routing

When `indexer.indexOverrideField` is set, documents are routed to different Solr collections within the same batch. The value of the override field on each document determines which collection receives it. On documents that have the override field, the field is removed before sending. Documents without it go to the default collection.

The indexer does not create collections.

---

## Interleaved Add/Delete Ordering

SolrIndexer preserves the order of adds and deletes within a batch. If a batch contains an add followed by a delete for the same document ID (or vice versa), the operations are sent to Solr in the correct sequence. This is important for incremental ingestion where a document may be re-added and then deleted (or deleted and re-added) within the same run.

Internally, the indexer flushes pending adds before queuing a delete for an ID that is already pending, and flushes pending deletes before queuing an add for an ID that is pending deletion. Any delete-by-query also forces a flush of pending adds, and pending delete-by-query requests are flushed before the next add.

---

## Delete-by-Query via Terms Queries

When `deleteByFieldField` and `deleteByFieldValue` are configured, SolrIndexer constructs Solr terms queries rather than issuing individual delete-by-query calls. Values for the same field are combined into a single terms query, and the queries for different fields (plus any pending deletes by ID) are joined with `OR` into one request:

```
(+{!terms f='category' v='obsolete,deprecated'})
```

This reduces the number of round-trips to Solr when many documents are marked for deletion in the same batch. Pending deletes by ID become a terms query on the `id` field. Because values are joined with commas, a value or ID that contains a comma will not match as intended.

---

## Errors

- If the final add request for a collection fails, the documents in that request are marked as failed.
- If any other request fails (a delete, or an add flushed early to preserve ordering), the whole batch fails. Requests already sent for that batch are not rolled back.
- A failed final add does not stop the deletes queued for that collection: they are still sent.
- Solr errors and communication failures are raised as retryable exceptions, carrying the HTTP status code when Solr returns one, so `indexer.retryableStatusCodes` can match them. The nested-object error below is not retryable.

---

## Limitations

- **Object fields not supported:** If a document field (or a child document field) contains a nested object, SolrIndexer throws an `IndexerException` and the whole batch fails. Flatten nested structures in your pipeline before indexing to Solr.

---

## Troubleshooting

**Connection validation fails:**
- **Single-node mode** (`useCloudClient: false`): Validation uses `solrClient.ping()`.
- **SolrCloud mode** (`useCloudClient: true`): Validation requests cluster status through the Collections API (`CLUSTERSTATUS`). If this fails, check ZooKeeper or Solr URL connectivity and that at least one Solr node is running.

With `indexer.sendEnabled: false`, no Solr client is created, connection validation always passes, and nothing is sent.

**"Object field ... is not supported by the SolrIndexer":**
A document field contains a nested JSON object. Add a stage to your pipeline that flattens or removes the field before it reaches the indexer.
