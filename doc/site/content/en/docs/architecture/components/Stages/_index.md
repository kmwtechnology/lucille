---
weight: 6
title: Stage
date: 2025-06-06
description: A Stage performs a specific transformation on a Document.
---

## What a Stage Does

A Stage is the fundamental unit of document transformation in Lucille. Each Stage performs a single, focused operation on a Document: extracting text, renaming fields, generating embeddings, looking up data from an external system, or any other enrichment task.

Stages are composed into Pipelines. When a Document flows through a Pipeline, it passes through each Stage in sequence. Each Stage receives the Document as mutated by all previous Stages and can read fields, write fields, or emit child documents.

## The Stage Contract

A Stage implementation must provide one method: `processDocument(Document doc)`. This method receives a Document, performs its transformation (typically by reading and writing fields), and returns an iterator of result documents. For most stages, the iterator contains just the input document (now modified). Stages that generate child documents return the children followed by the parent.

The framework handles everything else:
- **Instantiation** — Stages are created from class names in configuration via reflection.
- **Lifecycle** — `start()` is called once before processing begins (for resource acquisition); `stop()` is called once after processing ends (for cleanup).
- **Condition evaluation** — The framework checks conditions before calling `processDocument()`. If conditions are not met, the Stage is skipped entirely.
- **Thread isolation** — Each Worker thread gets its own Stage instance. No synchronization is needed.
- **Error handling** — If `processDocument()` throws, the framework catches the exception, marks the document as failed, and continues processing other documents.

## Conditions as a Design Decision

Rather than supporting sub-pipelines or branching, Lucille provides per-stage conditions that control whether a Stage applies to a given Document. This keeps the pipeline linear while allowing different processing for different document types.

Conditions are evaluated by the framework before invoking the Stage. A Stage author never implements conditional logic — they write a Stage that does one thing, and the configuration determines which documents it applies to. This separation means Stages are simpler to write, simpler to test, and reusable across pipelines with different condition configurations.

## Child Document Emission

A Stage can produce additional documents — children — that flow through the remaining pipeline stages independently. This is how Lucille handles 1-to-N fan-out (e.g., chunking a document into embedding-sized pieces). Children are tracked by the Publisher's accounting system and indexed as independent records.

The iterator-based return type (`Iterator<Document>`) means children are produced lazily. Memory usage is bounded regardless of how many children a Stage generates.

---

## Practical Guide

For how to configure stages — syntax, conditions, conditionPolicy, and the full catalogue of built-in stages — see [Stages]({{< relref "docs/reference/stages" >}}) in the Ingest Designer Guide.

For how to build a custom Stage, see [Developing New Components]({{< relref "docs/developer-guide/dev_new_components" >}}).
