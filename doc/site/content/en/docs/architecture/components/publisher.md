---
title: Publisher
date: 2025-10-23
description: Provides a way to publish Documents for processing by the pipeline.
---

*Publisher* provides a way to publish documents for processing by the pipeline. When published, a Lucille document becomes available for consumption by any active pipeline Worker.

The Publisher is aware of every document in the run that needs to be indexed, and determines when to end a run by reading all the specific document events.

Publisher also:
* Is responsible for stamping a designated `run_id` on each published document and maintaining accounting details specific to a run.
* Accepts incoming events related to published documents and their children (via in-memory queue or Kafka Event Topic).

