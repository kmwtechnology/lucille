---
title: "Indexing Fan-Out"
weight: 7
date: 2026-10-02
description: >
  Diagrams of how Lucille overlaps bulk requests against the search destination and how that
  concurrency fans out across threads, JVMs, and processes in local, hybrid, and distributed modes.
---

These diagrams show how Lucille overlaps bulk requests against the search
destination, and how that concurrency fans out across threads, JVMs, and processes.
They correspond to the levers described on the [Indexing Throughput]({{< relref "indexing-throughput" >}})
page: larger batches, more consumers, and more sending threads per consumer
(`indexer.maxConcurrentBatches`).

Two kinds of concurrency combine in every deployment:

- **Across consumers** - more indexers (local worker/indexer threads, hybrid
  `WorkerIndexer` threads, or standalone distributed indexer processes) each send
  their own bulk requests, and those requests overlap with the other consumers'.
- **Within a consumer** - a single indexer with `maxConcurrentBatches` greater than
  1 keeps several of its own bulk requests in flight at once, on its own small pool
  of send threads.

The total send concurrency against the destination is the product of the two:
roughly the consumer count times `maxConcurrentBatches` bulk requests in flight (and
a comparable amount of connection load), which is the number to size against the
destination's write capacity.

The scenarios below cover each deployment mode: **local** (one JVM, in-memory
queues, a single indexer), **hybrid** (`WorkerIndexer` threads reading a source
topic), and **fully distributed** (separate worker and indexer processes connected
through a destination topic). In every diagram the **destination cluster** is the
search backend - a multi-node OpenSearch, Elasticsearch, or Solr cluster.

## Local mode

In local mode there is no Kafka. The Runner launches the publisher, the pipeline
workers, and a single indexer as threads in one JVM, all sharing a `LocalMessenger`
that holds two in-memory queues: a **source queue** (publisher to workers) and a
**dest queue** (workers to indexer). Multiple worker threads pull from the source
queue and push processed documents to the dest queue; the one indexer thread polls
the dest queue and sends to the destination. `maxConcurrentBatches` is the only
lever that overlaps sends here, since there is a single indexer and no partitions.

### Local mode: 1 JVM, multiple worker threads, 1 indexer, maxConcurrentBatches = 4

```mermaid
flowchart LR
    subgraph JVM["Runner JVM (local mode)"]
        direction TB
        PUB["Publisher<br/>(connector output)"]

        SRCQ(["Source queue<br/>(in-memory)"])

        W0["Worker thread 0"]
        W1["Worker thread 1"]
        W2["Worker thread 2"]

        DESTQ(["Dest queue<br/>(in-memory)"])

        IDX["Indexer A"]

        PUB --> SRCQ
        SRCQ --> W0
        SRCQ --> W1
        SRCQ --> W2
        W0 --> DESTQ
        W1 --> DESTQ
        W2 --> DESTQ
        DESTQ --> IDX
    end

    DEST[("Destination cluster")]

    IDX --> RA1["Bulk Request A1"] --> DEST
    IDX --> RA2["Bulk Request A2"] --> DEST
    IDX --> RA3["Bulk Request A3"] --> DEST
    IDX --> RA4["Bulk Request A4"] --> DEST
```

## Hybrid mode

In hybrid mode each `WorkerIndexer` thread pairs a **worker** (the Kafka consumer,
subscribed to the **source** topic) with an **indexer** (which sends bulk requests
to the destination). The source topic's partition count gates how many threads can
be active across the consumer group; `maxConcurrentBatches` is per indexer, so
aggregate send concurrency and connection load are the thread count times
`maxConcurrentBatches`.

### Simple case: 1 JVM, 2 threads, maxConcurrentBatches = 2

Source topic with 2 partitions, one WorkerIndexer JVM running `worker.threads: 2`,
each indexer at `maxConcurrentBatches: 2`. Total in flight from the JVM:
2 threads x 2 = **4 concurrent bulk requests**.

```mermaid
flowchart LR
    subgraph SRC["Source topic (2 partitions)"]
        P0["Partition 0"]
        P1["Partition 1"]
    end

    subgraph JVM["WorkerIndexer JVM"]
        W0["Worker A<br/>(Kafka consumer)"] -->|in-JVM queue| I0["Indexer A"]
        W1["Worker B<br/>(Kafka consumer)"] -->|in-JVM queue| I1["Indexer B"]
    end

    I0 --> R0a["Bulk Request A1"] --> DEST
    I0 --> R0b["Bulk Request A2"] --> DEST
    I1 --> R1a["Bulk Request B1"] --> DEST
    I1 --> R1b["Bulk Request B2"] --> DEST

    DEST[("Destination cluster")]

    P0 -->|assigned to| W0
    P1 -->|assigned to| W1
```

### Scaled case: 2 JVMs, 2 threads each, 8 partitions, maxConcurrentBatches = 3

Source topic with 8 partitions, two WorkerIndexer JVMs running `worker.threads: 2`
each (4 threads total), each indexer at `maxConcurrentBatches: 3`. Because there are
more partitions than threads, Kafka assigns **multiple partitions to each worker** -
here each worker owns 2 partitions and feeds them to its one paired indexer.

Each arrow from a partition to a worker is a partition assignment; each `Bulk Request`
box is one in-flight bulk send, labeled with its indexer (A-D) and the request's slot
within that indexer. With `maxConcurrentBatches: 3`, every indexer has 3 of them in
flight at once. Send concurrency is set by the thread count and `maxConcurrentBatches`,
not the partition count: 4 threads x 3 = **12 concurrent bulk requests**. The extra
partitions add headroom to scale *out* later (up to 8 threads) without repartitioning.

```mermaid
flowchart LR
    subgraph SRC["Source topic (8 partitions)"]
        P0["Partition 0"]
        P1["Partition 1"]
        P2["Partition 2"]
        P3["Partition 3"]
        P4["Partition 4"]
        P5["Partition 5"]
        P6["Partition 6"]
        P7["Partition 7"]
    end

    subgraph JVMA["WorkerIndexer JVM 1"]
        WA0["Worker A"] --> IA0["Indexer A"]
        WA1["Worker B"] --> IA1["Indexer B"]
    end

    subgraph JVMB["WorkerIndexer JVM 2"]
        WB0["Worker C"] --> IB0["Indexer C"]
        WB1["Worker D"] --> IB1["Indexer D"]
    end

    IA0 --> RA01["Bulk Request A1"] --> DEST
    IA0 --> RA02["Bulk Request A2"] --> DEST
    IA0 --> RA03["Bulk Request A3"] --> DEST

    IA1 --> RA11["Bulk Request B1"] --> DEST
    IA1 --> RA12["Bulk Request B2"] --> DEST
    IA1 --> RA13["Bulk Request B3"] --> DEST

    IB0 --> RB01["Bulk Request C1"] --> DEST
    IB0 --> RB02["Bulk Request C2"] --> DEST
    IB0 --> RB03["Bulk Request C3"] --> DEST

    IB1 --> RB11["Bulk Request D1"] --> DEST
    IB1 --> RB12["Bulk Request D2"] --> DEST
    IB1 --> RB13["Bulk Request D3"] --> DEST

    DEST[("Destination cluster")]

    P0 --> WA0
    P1 --> WA0
    P2 --> WA1
    P3 --> WA1
    P4 --> WB0
    P5 --> WB0
    P6 --> WB1
    P7 --> WB1
```

## Fully distributed mode

In distributed mode the workers and indexers run as separate processes, connected
through Kafka. The workers consume the source topic and publish processed documents
to the **destination topic**; the standalone indexers are the Kafka consumers of the
destination topic. The destination topic's partition count caps how many indexers can
be active, and `maxConcurrentBatches` overlaps sends within each indexer.

### Case 1: single-partition destination topic, one indexer, maxConcurrentBatches = 4

Several worker processes publish to a destination topic that has **one** partition.
Only one indexer in the consumer group can be assigned that partition, so a single
indexer consumes everything. Adding more indexer processes yields no send parallelism
(they sit idle). `maxConcurrentBatches` is the only lever that overlaps sends - the
same situation that the delete-by-query constraint forces.

```mermaid
flowchart LR
    subgraph WORKERS["Worker processes"]
        WP0["Worker process 0"]
        WP1["Worker process 1"]
        WP2["Worker process 2"]
    end

    subgraph DT["Destination topic"]
        DP0["Partition 0"]
    end

    subgraph IP["Indexer process"]
        IDX["Indexer A"]
    end

    DEST[("Destination cluster")]

    WP0 --> DP0
    WP1 --> DP0
    WP2 --> DP0

    DP0 -->|assigned to| IDX

    IDX --> RA1["Bulk Request A1"] --> DEST
    IDX --> RA2["Bulk Request A2"] --> DEST
    IDX --> RA3["Bulk Request A3"] --> DEST
    IDX --> RA4["Bulk Request A4"] --> DEST
```

### Case 2: 2-partition destination topic, two indexer processes, maxConcurrentBatches = 4

The destination topic has **2** partitions, so two standalone indexer processes can
each be assigned one partition and consume concurrently. Each indexer additionally
overlaps its own sends with `maxConcurrentBatches`. Total in flight:
2 indexers x 4 = **8 concurrent bulk requests** at `maxConcurrentBatches: 4`.

```mermaid
flowchart LR
    subgraph WORKERS["Worker processes"]
        WP0["Worker process 0"]
        WP1["Worker process 1"]
    end

    subgraph DT["Destination topic"]
        DP0["Partition 0"]
        DP1["Partition 1"]
    end

    subgraph IP0["Indexer process A"]
        IDX0["Indexer A"]
    end

    subgraph IP1["Indexer process B"]
        IDX1["Indexer B"]
    end

    DEST[("Destination cluster")]

    WP0 --> DP0
    WP1 --> DP1
    WP0 --> DP1
    WP1 --> DP0

    DP0 -->|assigned to| IDX0
    DP1 -->|assigned to| IDX1

    IDX0 --> RA1["Bulk Request A1"] --> DEST
    IDX0 --> RA2["Bulk Request A2"] --> DEST
    IDX0 --> RA3["Bulk Request A3"] --> DEST
    IDX0 --> RA4["Bulk Request A4"] --> DEST

    IDX1 --> RB1["Bulk Request B1"] --> DEST
    IDX1 --> RB2["Bulk Request B2"] --> DEST
    IDX1 --> RB3["Bulk Request B3"] --> DEST
    IDX1 --> RB4["Bulk Request B4"] --> DEST
```
