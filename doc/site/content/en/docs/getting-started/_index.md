---
title: Getting Started
weight: 2
---

## Prerequisites
* Java development environment with Java 17 or later
* Recent Maven version

## Installation
Start by cloning the repository:

`git clone https://github.com/kmwtechnology/lucille.git`

At the top level of the project, run:

`mvn clean install`


## Try it out
Lucille includes a few examples in the `lucille-examples` module to help you get started.

To see how to ingest the contents of a local CSV file into an instance of Apache Solr, refer to `lucille-examples/simple-csv-solr-example`.

To run this example, start an instance of Apache Solr on port 8983 and create a collection called `quickstart`. For more information about how to use Solr, see the [Apache Solr Reference Guide](https://solr.apache.org/guide/solr/latest/getting-started/introduction.html)).

Go to `lucille-examples/lucille-simple-csv-solr-example` in your working copy of Lucille and run:

`mvn clean install`

`./scripts/run_ingest.sh`

This script executes Lucille with a configuration file named `simple-csv-solr-example.conf` that tells Lucille to read a CSV of top songs and send each row as a document to Solr.

Run a commit with `openSearcher=true` on your `quickstart` collection to make the documents visible. Go to your Solr admin dashboard, execute a `*:*` query and you should see the songs from the source file now visible as Solr documents.

