---
title: Stages
weight: 4
date: 2025-06-06
description: Catalogue of built-in stages and how to configure them.
---

For conceptual documentation — what a Stage is, the Stage contract, conditions as a design decision, and child document emission — see [Architecture: Stage]({{< relref "docs/architecture/components/Stages" >}}).

## Configuring a Stage

To configure a Stage, provide its `class` in the config. You can also specify a `name` (for logging and error messages), `conditions`, and `conditionPolicy`:

```hocon
{
  name: "AddRandomBoolean-First"
  class: "com.kmwllc.lucille.stage.AddRandomBoolean"
  field_name: "rand_bool_1"
  percent_true: 65
}
```

Each Stage also accepts its own implementation-specific parameters (like `field_name` and `percent_true` above). See the individual stage pages below for details.

---

## Conditions

For any Stage, you can specify `conditions` in its config to control when the Stage processes a Document. Each condition has a required parameter, `fields`, and two optional parameters, `operator` and `values`.

| Parameter | Required | Description |
|---|---|---|
| `fields` | Yes | List of field names that will determine whether the Stage applies to a Document. |
| `values` | No | List of values to search for in those fields. If not specified, only field *existence* is checked. |
| `operator` | No | `"must"` (default) or `"must_not"`. |

In the root of the Stage's config, specify `conditionPolicy` to control how multiple conditions combine:
- `"any"` (default) — at least one condition must be met
- `"all"` — all conditions must be met

**Example:** Run the `Print` Stage only when `city` equals `"Boston"` or `"New York"`:

```hocon
{
  name: "print-1"
  class: "com.kmwllc.lucille.stage.Print"
  conditionPolicy: "any"
  conditions: [
    {
      fields: ["city"]
      values: ["Boston", "New York"]
    }
  ]
}
```

**Example:** Only embed documents that have both `content` and `content_type = "article"`:

```hocon
{
  class: "com.kmwllc.lucille.stage.OpenAIEmbed"
  fields: ["content"]
  dest: ["content_vector"]
  apiKey: ${OPENAI_API_KEY}
  conditionPolicy: "all"
  conditions: [
    { fields: ["content"] },
    { fields: ["content_type"], values: ["article"] }
  ]
}
```

---

## Stage Catalogue

See [All Stages]({{< relref "docs/reference/stages/stages_reference" >}}) for a complete listing of all available stages organized by category, including their configuration parameters.

Detailed pages are available for more complex stages:

- [ChunkText]({{< relref "docs/reference/stages/chunk_text" >}}) — Split long text fields into chunks for embedding and RAG pipelines.
- [EmbeddedPython]({{< relref "docs/reference/stages/embedded_python" >}}) — Run Python code inside the JVM using GraalPy.
- [ExternalPython]({{< relref "docs/reference/stages/external_python" >}}) — Delegate processing to an external Python process via Py4J.
- [PromptOllama]({{< relref "docs/reference/stages/prompt_ollama" >}}) — Enrich documents using a locally-running LLM.
- [QueryOpensearch]({{< relref "docs/reference/stages/query_opensearch" >}}) — Execute OpenSearch search templates per document.

Plugin stages (TextExtractor, ApplyOCR, ApplyOpenNLPNameFinders, JlamaEmbed) are listed at the bottom of [All Stages]({{< relref "docs/reference/stages/stages_reference" >}}) with their Maven dependencies.
