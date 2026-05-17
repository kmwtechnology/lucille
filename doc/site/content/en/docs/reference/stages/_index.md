---
title: Stages
weight: 2
date: 2025-06-06
description: Catalogue of built-in stages shipped with Lucille.
---

For conceptual documentation — what a Stage is, the Stage contract, conditions, child document emission, and how to build a custom Stage — see [Architecture: Stages]({{< relref "docs/architecture/components/Stages" >}}).

## Stage Catalogue

See [All Stages]({{< relref "docs/reference/stages/stages_reference" >}}) for a complete listing of all available stages organized by category, including their configuration parameters.

Detailed pages are available for more complex stages:

- [ChunkText]({{< relref "docs/reference/stages/chunk_text" >}}) — Split long text fields into chunks for embedding and RAG pipelines.
- [EmbeddedPython]({{< relref "docs/reference/stages/embedded_python" >}}) — Run Python code inside the JVM using GraalPy.
- [ExternalPython]({{< relref "docs/reference/stages/external_python" >}}) — Delegate processing to an external Python process via Py4J.
- [PromptOllama]({{< relref "docs/reference/stages/prompt_ollama" >}}) — Enrich documents using a locally-running LLM.
- [QueryOpensearch]({{< relref "docs/reference/stages/query_opensearch" >}}) — Execute OpenSearch search templates per document.

Plugin stages (TextExtractor, ApplyOCR, ApplyOpenNLPNameFinders, JlamaEmbed) are listed at the bottom of [All Stages]({{< relref "docs/reference/stages/stages_reference" >}}) with their Maven dependencies.
