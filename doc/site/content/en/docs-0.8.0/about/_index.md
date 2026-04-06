---
title: About Lucille
weight: 1
description: >
  Understanding what Lucille is, why you should use it, and how it works.
---

## What is Lucille?

Lucille is a production-grade Search ETL solution designed to efficiently get data into Lucene-based search engines such as Elasticsearch, OpenSearch, and Solr as well as vector databases such as Pinecone & Weaviate.  Lucille enables complex processing of documents before they are indexed by search engine, freeing up resources that can be used by the search engine to compute queries with greater speed.

Lucille is Java-based and open-source. Lucille supports batch, incremental, and streaming data ingestion architectures.

## Why use Lucille?
Search ETL is a category of ETL problem where data must be extracted from a source system, transformed, and loaded into a *search engine*.

A Search ETL solution must speak the language of search: it must represent data in the form of search-engine-ready Documents, it must know how to *enrich* Documents to support common search use cases, and it must follow best practices for interacting with search engines including support for batching, routing, and versioning.

To be production-grade, a search ETL solution must be scalable, reliable, and easy to use. It should support parallel Document processing, it should be observable, it should be easy to configure, it should have extensive test coverage, and it should have been hardened through multiple challenging real-world deployments.

Lucille handles all of these things so you don't have to. Lucille helps you get your data into Lucene-based search engines like Apache Solr, Elasticsearch, or OpenSearch as well as vector-based search engines like Pinecone and Weaviate, and it helps you keep that search engine content up-to-date as your backend data changes. Lucille does this in a way that scales as your data volume grows, and in a way that's easy to evolve as your data transformation requirements become more complex. Lucille implements search best practices so you can stay focused on your data itself and what you want to do with it.

## How Does Lucille Work?
The basic architectural ideas of Lucille are as follows:

1. A *Connector* retrieves data from a source system.
2. *Worker(s)* enrich the data.
3. *Indexer(s)* index the data into a search engine.
4. These three core components (Connectors, Workers, and Indexers) run concurrently and communicate with each other using a messaging framework.
    -  The core components can function as threads inside a JVM, allowing for all of Lucille run as a single Java process, allowing for a simple and easy deployment model.
    -  The core components can function as standalone Java processes communicating through an external Apache Kafka message broker, allowing for massive scale.
5. Documents are enriched en-route to the search engine using a *Pipeline* built from composable processing *Stages*. The pipeline is configuration-driven.
6. Document lifecycle events (such as creation, indexing, and erroring out) are tracked so that the framework can determine when all the work in a batch ingest is complete.


## More Information

The Lucille project is developed and maintained by KMW Technology ([kmwllc.com](https://kmwllc.com/)).
For more information regarding Lucille, please [contact us](https://kmwllc.com/index.php/contact-us/).



