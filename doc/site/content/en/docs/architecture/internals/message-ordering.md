---
title: Message Ordering
weight: 6
date: 2025-06-09
description: >
  How Kafka keys preserve operation order across distributed components, and why WorkerIndexer pairs 1:1.
---

## Why Ordering Matters

Search ingestion often involves sequences of operations on the same document: a create, followed by one or more updates, possibly followed by a delete. If these operations are reordered, the final state of the search index is wrong. For example, if a sequence of create → update → delete is reordered so that the delete is processed before the update, the document survives in the index when it should have been removed. Or if the create arrives after the delete, the document reappears.

Lucille guarantees that operations on the same document ID are processed and indexed in the order they were published, even when multiple Workers and Indexers run concurrently.

---

## The Mechanism: Document ID as Kafka Key

Lucille uses the document ID as the Kafka message key at every stage of the pipeline:

- **Publisher → processing topic:** `new ProducerRecord(sourceTopicName, document.getId(), document)`
- **Worker → indexing topic:** `new ProducerRecord(destTopicName, document.getId(), document)`
- **Worker/Indexer → event topic:** `new ProducerRecord(eventTopicName, event.getDocumentId(), event)`

Kafka's partitioning guarantee is: messages with the same key are always routed to the same partition, and messages within a partition are consumed in order by a single consumer. This means that all operations for a given document ID land on the same partition at each stage, and are consumed sequentially by whichever component owns that partition.

---

## Ordering in Fully Distributed Mode

In fully distributed mode, Workers and Indexers are separate processes, each consuming from Kafka topics via consumer groups.

**Processing topic → Workers:**
All messages for document ID "D" land on the same partition P of the processing topic. Kafka assigns partition P to exactly one Worker in the consumer group. That Worker consumes messages from P in order, processing them sequentially (one at a time). The Worker will not pick up the second message for "D" until it has finished processing the first.

**Indexing topic → Indexers:**
When the Worker produces processed documents to the indexing topic, it again uses `document.getId()` as the key. All messages for document "D" land on the same partition Q of the indexing topic. Kafka assigns partition Q to exactly one Indexer in the consumer group. That Indexer consumes messages from Q in order.

**The ordering guarantee holds end-to-end** because:
1. Same document ID → same partition (at each topic), guaranteed by Kafka's partitioner.
2. Same partition → same consumer (Worker or Indexer), guaranteed by Kafka's consumer group protocol.
3. Same consumer → sequential processing, guaranteed by the single-threaded polling loop in each component.

**Multiple Indexers do not break ordering.** Even with N Indexers running, each Indexer owns a disjoint set of partitions. All messages for document "D" are on partition Q, and only one Indexer consumes from Q. There is no shared queue where multiple Indexers could pick up different parts of a sequence for the same document.

---

## Ordering in WorkerIndexer Mode

In WorkerIndexer mode, a Worker and an Indexer are paired in the same JVM, communicating via an in-memory `LinkedBlockingQueue` rather than a Kafka topic between them.

**Why the 1:1 pairing preserves ordering:**
Each WorkerIndexer pair owns one or more Kafka partitions of the processing topic exclusively. All operations for a given document ID land on the same partition, so they are consumed by the same Worker, processed in order, and placed on the in-memory queue in order. The paired Indexer reads from that queue (which is FIFO) and indexes them in order.

**What would break ordering without the 1:1 pairing:**
If multiple Worker threads wrote to a single shared in-memory indexing queue, the ordering of messages *for the same document ID* would still be preserved — because all messages for the same ID come from the same partition, consumed by the same Worker thread, and placed on the shared queue in order. Interleaving of messages for *different* document IDs is not a problem for ordering.

The real problem arises on the consumption side: if multiple Indexer threads read from that shared queue, different Indexers could consume different portions of an ordered sequence for the same document ID. Indexer A might pick up the create, Indexer B might pick up the delete, and Indexer B might send its batch to the search backend before Indexer A does — resulting in the document surviving when it should have been deleted.

In fully distributed mode, this problem does not arise even with multiple Indexers, because the indexing topic is a Kafka topic with partition-based consumer assignment. All messages for document "D" are on the same partition, consumed by the same Indexer. But in WorkerIndexer mode, the "indexing queue" is an in-memory `LinkedBlockingQueue` — it has no concept of partitions or consumer assignment. Any thread reading from it can pick up any message. This is why the 1:1 pairing is necessary: with exactly one Indexer reading from the queue, all messages are consumed in FIFO order, preserving the ordering that the Worker established.

---

## Why WorkerIndexer Pairs Each Worker with a Dedicated Indexer

The 1:1 pairing serves two purposes: **offset management** and **message ordering**.

### Offset Management

The Worker reads from Kafka and should only commit an offset after the document has been indexed — not just processed. The mechanism:

1. The Worker reads a document from Kafka (noting its topic/partition/offset).
2. The Worker processes it and places it on the in-memory queue.
3. The paired Indexer picks it up, batches it, and sends the batch to the search backend.
4. After the batch succeeds, the Indexer places the offsets of the indexed documents on an in-memory offset queue.
5. The Worker reads from that offset queue and commits those offsets back to Kafka.

This closed loop between one Worker and one Indexer is simple because there's no ambiguity about which Worker should commit which offset. If multiple Workers shared an Indexer, the Indexer would need to route offset information back to the correct Worker — adding coordination complexity.

### Message Ordering

As described above, the in-memory queue between Worker and Indexer has no partition semantics. With a single Indexer consuming from it, FIFO order is preserved. With multiple Indexers, ordering would be lost for documents whose operations span multiple batch flushes.

### The Tradeoff

The 1:1 pairing means you cannot independently scale Workers and Indexers within a single process — they scale together as pairs (via `WorkerIndexerPool`). If you need independent scaling (e.g., many Workers but few Indexers, or vice versa), use fully distributed mode with separate Worker and Indexer processes communicating via Kafka topics, where Kafka's partition-based assignment provides both ordering and independent scaling.

---

## When Could Ordering Break?

**Consumer group rebalance.** If Kafka triggers a rebalance (e.g., a Worker or Indexer is slow, or a new instance joins the group), partitions may be reassigned. A document that was in-flight on the old consumer might be redelivered to the new consumer. This can cause a duplicate — but not a reordering, because the duplicate appears later in the partition's log than the original.

**Topic partition count changes.** Kafka's default partitioner hashes the key modulo the partition count. If the partition count changes mid-ingest, the same document ID might hash to a different partition than before. Operations published before the change and operations published after could end up on different partitions, breaking the ordering guarantee. In practice, partition counts should not be changed during an active ingest.

**Processing time variance does not break ordering.** Even if one document takes much longer to process than another, ordering is preserved because the Worker processes messages from a partition sequentially. It does not start the next message until the current one is complete.

---

## Summary

| Deployment Mode | Ordering Mechanism | Why It Works |
|---|---|---|
| Fully distributed | Kafka partition assignment at both hops | Same doc ID → same partition → same consumer at each stage |
| WorkerIndexer | Kafka partition assignment + FIFO in-memory queue | Same doc ID → same partition → same Worker → single Indexer reads FIFO |
| Local (single JVM) | In-memory queues with single Worker/Indexer threads | Sequential processing within each thread; no concurrency to reorder |

The ordering guarantee holds as long as:
- Document ID is used as the Kafka key (always true in Lucille).
- Partition counts remain stable during an ingest.
- No consumer group rebalance causes partition reassignment mid-sequence (rare, and results in duplicates rather than reordering).
