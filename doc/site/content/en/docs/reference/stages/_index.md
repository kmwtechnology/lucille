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

For any Stage, you can specify `conditions` in its config to control when the Stage processes a Document.

### Condition parameters

| Parameter | Required | Description |
|---|---|---|
| `fields` | Yes | One or more field names to evaluate. |
| `values` | No | List of values to match against those fields. If omitted, only field existence is checked. |
| `valuesPath` | No | Path to a file containing match values, one per line. Use instead of `values` when the list is large or managed externally. Supports local paths, `classpath:` resources, and cloud storage URIs (S3, GCS, HTTPS). |
| `operator` | No | `"must"` (default) — condition passes if a match is found. `"must_not"` — condition passes if no match is found. |

`values` and `valuesPath` are mutually exclusive — specifying both is an error.

### How matching works

**With `values` or `valuesPath`:** The condition passes if any of the listed fields contains any of the listed values. Matching is type-coerced to string — a boolean field `true` matches the value `"true"`, an integer `10` matches `"10"`. `null` is a valid value entry and will match a null field value.

**Without `values` or `valuesPath`:** The condition checks field existence only.
- `operator: "must"` — passes if **all** listed fields are present on the document.
- `operator: "must_not"` — passes if **all** listed fields are absent from the document.

### conditionPolicy

When a stage has multiple conditions, `conditionPolicy` in the stage's root config controls how they combine:
- `"all"` (default) — all conditions must be met
- `"any"` — at least one condition must be met

### Examples

**Run a stage only when a field exists:**

```hocon
{
  class: "com.kmwllc.lucille.stage.MyStage"
  conditions: [
    { fields: ["content"] }
  ]
}
```

**Run a stage only when a field matches a value:**

```hocon
{
  name: "print-1"
  class: "com.kmwllc.lucille.stage.Print"
  conditions: [
    { fields: ["city"], values: ["Boston", "New York"] }
  ]
}
```

**Skip a stage when a field is present (`must_not` existence check):**

```hocon
{
  class: "com.kmwllc.lucille.stage.OpenAIEmbed"
  conditions: [
    { fields: ["embedding"], operator: "must_not" }
  ]
}
```

**Require multiple conditions (all must be met):**

```hocon
{
  class: "com.kmwllc.lucille.stage.OpenAIEmbed"
  conditionPolicy: "all"
  conditions: [
    { fields: ["content"] }
    { fields: ["content_type"], values: ["article"] }
  ]
}
```

**Load match values from a file:**

```hocon
{
  class: "com.kmwllc.lucille.stage.DropDocument"
  conditions: [
    { fields: ["category"], valuesPath: "s3://my-bucket/excluded-categories.txt" }
  ]
}
```

For the full reference on controlling document fate and connector sequencing — conditions, skipping, dropping, error handling, child documents, and more — see [Control Flow]({{< relref "docs/reference/control-flow" >}}).

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
