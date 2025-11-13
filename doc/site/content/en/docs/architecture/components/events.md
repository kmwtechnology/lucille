---
title: Events
date: 2025-10-23
description: As Lucille runs, it generates Events that inform on success, failure, etc.
---

## Lucille Events
As a document passes through various stages of the Lucille ETL pipeline, and as the documents are handled by the indexer, Event messages are generated. 

Connectors listen for these events to ensure that all of the documents sent for processing are successfully processed and accounted for. 

Errors during processing are reported and tracked via these event messages so the connector can report back the overall success or failure of the execution.

## Event Topics
A Kafka Topic that contains event messages. The event messages are sent from stages in the Lucille Pipeline as well as from the indexer.

The event topic name is based on a run ID which is stamped on documents that are flowing through the system. Whenever events are enabled, documents need to have a run ID on them. 

In the case where there is no runner to create the run ID and no Lucille publisher to stamp that run ID on the documents, the “third party publisher” that is putting document json onto Kafka would need to include a run ID on those documents. It could choose its own run ID to use.
