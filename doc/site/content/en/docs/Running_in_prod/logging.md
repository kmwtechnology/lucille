---
title: Logging
date: 2025-05-07
description: Interpreting and using Lucille logs.
---

### Lucille Logs

**There are many ways you can use the logs output by Lucille.** Lucille has, essentially, two main loggers for tracking your Lucille run: 
the `Root` logger, and the `DocLogger`. 

##### The Root Logger
The `Root` logger outputs log statements from a variety of sources, allowing you to track your Lucille run. For example, the `Root` logger
is where you'll get intermittent updates about your pipeline's performance, warnings from Stages or Indexers in certain situations, etc.

##### The Doc Logger
The `DocLogger` is very verbose - it tracks the lifecycle of _each Document_ in your Lucille pipeline.
For example, a log statement is made when a Document is created, before it is published, before & after a Stage operates on it... etc. 
As you can imagine, this results in _many_ log statements - it is recommended these logs are stored in a file, rather than just
having them printed to the console.
Logs from the `DocLogger` will primarily be `INFO`-level logs - very rarely, an `ERROR`-level log will be made for a Document.

### Log Files
Lucille can store logs in a file as plain text or as JSON objects. When storing logs as JSON, each line will be a JSON object representing
a log statement in accordance with the `EcsLayout`. By modifying the `log4j2.xml`, you can control which Loggers are enabled/disabled,
where their logs get stored, and what level of logs you want to process.

### Logstash & OpenSearch Dashboards
If you store your logs as JSON, you can easily run Logstash on the file(s), allowing you to index them into a Search Engine of your
choice for enhanced discovery and analysis. This can be particularly informative when working with the `DocLogger`. For example,
you might:
* Trace a specific Document's lifecycle by querying by the Document's ID.
* Using the Timestamp of the logs, track the performance of your Lucille pipeline and identify potential bottlenecks.
* Create Dashboards, allowing you to monitor your pipeline for potential warnings / errors for a repeated Lucille run.

Here is an example `pipeline.conf` for ingesting your Lucille logs into a local OpenSearch instance:

```
input {
  file {
    path => "/lucille/lucille-examples/lucille-simple-csv-solr-example/log/com.kmwllc.lucille-json*"
    mode => "read"
    codec => "json"
    start_position => "beginning"
    sincedb_path => "/dev/null"
    exit_after_read => "true"
  }
}
output {
  stdout {
    codec => rubydebug
  }
  opensearch {
    hosts => "http://localhost:9200"
    index => "logs"
    ssl_certificate_verification => false
    ssl => false
  }
}
```

Note that this pipeline will delete the log files after they are ingested.

Here are some queries you might run (using curl):

```json
curl -XGET "http://localhost:9200/logs/_search" -H 'Content-Type: application/json' -d '{
  "query": {
    "term": {
	  "id.keyword": "songs.csv-1"
    }
  }
}'
```

This query will only return log statements where the id of the Document being processed is "songs.csv-1", a Document ID
from the Lucille CSV example. This allows you to easily track the lifecycle of the Document as it was processed and published.

```json
curl -XGET "http://localhost:9200/logs/_search" -H 'Content-Type: application/json' -d '{
  "query": {
    "match": {
      "message": "FileHandler"        
    }
  }
}'
```

This query will return log statements with "FileHandler" in the message. This allows you to track specifically when Documents were
created from a JSON, CSV, or XML FileHandler.