---
weight: 1
title: Document
date: 2025-10-23
description: The basic unit of data that is sent through a Pipeline and eventually indexed into a search engine.
---

Search engines are designed to handle messy, incomplete, heterogeneous, loosely structured data. The basic unit of data in a search engine is typically called a "document" — it is simply a set of named fields, where each field may hold a single value or a list of values.

In Lucille, a Document is the basic unit of data that flows through a pipeline and gets indexed. Lucille's Document aims to be as close to a search engine document as possible. The idea is that you don't want to wait until the last minute to convert your data into a search-engine-friendly representation; you want to start with a search-engine-friendly representation the moment you acquire data from the source system, and use that representation throughout the entire enrichment pipeline. This means no intermediate object model, no mapping layer at the end — just fields in, fields out.

### Why not POJOs?

If you're coming from a background where encapsulation and strong typing are second nature, your first instinct might be to define POJOs for each entity type — a `Product`, a `SupportTicket`, a `LegalDocument` — with transformation logic behind well-named methods. That approach works when your domain has a small number of well-understood record types with stable schemas.

Search ingestion rarely looks like that. A typical project pulls data from multiple source systems, each with its own schema. The fields vary wildly from one record to the next — a database row has structured columns, a PDF has extracted text and metadata, a JSON API response has nested objects. Even within a single source, records are often inconsistent: optional fields that are sometimes present and sometimes not, multi-valued fields with unpredictable cardinality, fields whose meaning changes depending on the record type. Trying to capture all of this in a POJO hierarchy quickly becomes impractical — you end up with dozens of classes, most of which are just bags of optional fields, and the type system works against you rather than for you.

The pragmatic alternative is a generic map — something like `Map<String, Object>`. This gives you the flexibility to handle arbitrary fields, but at a cost: no typed access, no distinction between single-valued and multi-valued fields, verbose null-checking on every read, manual serialization logic at every boundary, and no built-in support for the update patterns (overwrite, append, skip) that search ingestion requires constantly.

Lucille's Document is the middle ground. It has the flexibility of a map — any field name, any number of fields, no fixed schema — but with a purpose-built API that eliminates the boilerplate. Typed getters, uniform single/multi-valued access, update modes as first-class operations, and zero-cost JSON serialization because the document is already in the format that search engines expect. You get the adaptability of a schemaless representation without giving up the ergonomics of a well-designed API.
