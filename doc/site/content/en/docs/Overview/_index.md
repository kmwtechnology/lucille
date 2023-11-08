---
title: Overview
description: Here's where your user finds out if your project is for them.
weight: 1
---

{{% pageinfo %}}
This is a placeholder page that shows you how to use this template site.
{{% /pageinfo %}}


# Lucille

Lucille is a production-grade Search ETL solution.

Search ETL is a category of ETL problem where data must be extracted from a source system, transformed, and loaded into a *search engine*.

A Search ETL solution must speak the language of search: it must represent data in the form of search-engine-ready Documents, it must know how to *enrich* Documents to support common search use cases, and it must follow best practices for interacting with search engines including support for batching, routing, and versioning.

To be production-grade, a search ETL solution must be scalable, reliable, and easy to use. It should support parallel Document processing, it should be observable, it should be easy to configure, it should have extensive test coverage, and it should have been hardened through multiple challenging real-world deployments.

Lucille handles all of these things so you don't have to. Lucille helps you get your data into Lucene-based search engines like Apache Solr, Elasticsearch, or OpenSearch as well as vector-based search engines like Pinecone and Weaviate, and it helps you keep that search engine content up-to-date as your backend data changes. Lucille does this in a way that scales as your data volume grows, and in a way that's easy to evolve as your data transformation requirements become more complex. Lucille implements search best practices so you can stay focused on your data itself and what you want to do with it.

## Installation

To use Lucille you will need a Java development environment with Java 11 or later and a recent version of Maven. Start by cloning the repository:

`git clone https://github.com/kmwtechnology/lucille.git`

At the top level of the project, run:

`mvn clean install`


## Getting Started

Lucille includes a few examples in the `lucille-examples` module to help you get started.

To see how to ingest the contents of a local CSV file into an instance of Apache Solr, refer to `lucille-examples/simple-csv-solr-example`.

To run this example, start an instance of Apache Solr on port 8983 and create a collection called `quickstart`. For more information about how to use Solr, see the [Apache Solr Reference Guide](https://solr.apache.org/guide/solr/latest/getting-started/introduction.html)).

Go to `lucille-examples/lucille-simple-csv-solr-example` in your working copy of Lucille and run:

`./scripts/run_ingest.sh`

This script executes Lucille with a configuration file named `simple-csv-solr-example.conf` that tells Lucille to read a CSV of top songs and send each row as a document to Solr.

Run a commit with `openSearcher=true` on your `quickstart` collection to make the documents visible. Go to your Solr admin dashboard, execute a `*:*` query and you should see the songs from the source file now visible as Solr documents.

---

## More Information

The Lucille project is developed and maintained by KMW Technology ([kmwllc.com](https://kmwllc.com/)).
For more information regarding Lucille, please [contact us](https://kmwllc.com/index.php/contact-us/).







The Overview is where your users find out about your project. Depending on the size of your docset, you can have a separate overview page (like this one) or put your overview contents in the Documentation landing page (like in the Docsy User Guide).

Try answering these questions for your user in this page:

## What is it?

Introduce your project, including what it does or lets you do, why you would use it, and its primary goal (and how it achieves it). This should be similar to your README description, though you can go into a little more detail here if you want.

## Why do I want it?

Help your user know if your project will help them. Useful information can include:

* **What is it good for?**: What types of problems does your project solve? What are the benefits of using it?

* **What is it not good for?**: For example, point out situations that might intuitively seem suited for your project, but aren't for some reason. Also mention known limitations, scaling issues, or anything else that might let your users know if the project is not for them.

* **What is it *not yet* good for?**: Highlight any useful features that are coming soon.

## Where should I go next?

Give your users next steps from the Overview. For example:

* [Getting Started](/docs/getting-started/): Get started with $project
* [Examples](/docs/examples/): Check out some example code!

