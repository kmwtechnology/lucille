---
title: Indexer
date: 2025-06-09
description: An Indexer sends processed Documents to a specific destination.
---

## Indexers

An indexer sends processed Documents to a specific destination. Only one Indexer can be defined in a Lucille run. 
All pipelines will feed to the same Indexer.

Indexer configuration has two parts: the generic `indexer` configuration, and configuration for the implementation you are using.
For example, if you are using Solr, you'd provide `solr` config, or `elastic` for Elasticsearch, `csv` for CSV, etc.

Here's what using the SolrIndexer might look like:
```hocon
# Generic indexer config
indexer {
  type: "solr"
  ignoreFields: ["city_temp"]
  batchSize: 100
}
# Specific implementation (Solr) config
solr {
  useCloudClient: true
  url: "localhost:9200"
  defaultCollection: "test_index"
}
```

At a minimum, `indexer` must contain either `type` or `class`. `type` is shorthand for an indexer provided by `lucille-core` -
it can be `"Solr"`, `"OpenSearch"`, `"ElasticSearch"`, or `"CSV"`. `indexer` can contain a variety of additional properties as well. 
Some Indexers do not support certain properties, however. For example, `OpenSearchIndexer` and `ElasticsearchIndexer` do not support
`indexer.indexOverrideField`.

The `lucille-core` module contains a number of commonly used indexers. Additional indexers with a large number of dependencies are provided as optional plugin modules.

### Lucille Indexers (Core)

* [Solr Indexer](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/indexer/SolrIndexer.java)
* [OpenSearch Indexer](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/indexer/OpenSearchIndexer.java)
* [Elasticsearch Indexer](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/indexer/ElasticsearchIndexer.java)
* [CSV Indexer](https://github.com/kmwtechnology/lucille/blob/main/lucille-core/src/main/java/com/kmwllc/lucille/indexer/CSVIndexer.java)

### Lucille Indexers (Plugins)

* [Pinecone Indexer](https://github.com/kmwtechnology/lucille/blob/main/lucille-plugins/lucille-pinecone/src/main/java/com/kmwllc/lucille/pinecone/indexer/PineconeIndexer.java)
* [Weaviate Indexer](https://github.com/kmwtechnology/lucille/blob/main/lucille-plugins/lucille-weaviate/src/main/java/com/kmwllc/lucille/weaviate/indexer/WeaviateIndexer.java)