---
title: Stages
date: 2025-06-06
description: A Stage performs a specific transformation on a Document.
---

## Lucille Stages

Stages are the building blocks of a Lucille pipeline. Each Stage performs a specific transformation on a Document.

To configure a stage, provide its class (under `class`) in its config. You can also specify a `name` for the Stage,
in addition to `conditions` and `conditionPolicy` (described below).

```hocon
{
  name: "AddRandomBoolean-First"
  class: "com.kmwllc.lucille.stage.AddRandomBoolean"
  field_name: "rand_bool_1"
  percent_true: 65
}
```

## Stage Reference

See the [Stage Reference]({{< relref "docs/architecture/components/Stages/stages_reference" >}}) for a complete listing of all available stages organized by category, including their configuration parameters.

Detailed pages are available for more complex stages:

- [EmbeddedPython]({{< relref "docs/architecture/components/Stages/embedded_python" >}}) — Run Python code inside the JVM using GraalPy.
- [ExternalPython]({{< relref "docs/architecture/components/Stages/external_python" >}}) — Delegate processing to an external Python process via Py4J.
- [PromptOllama]({{< relref "docs/architecture/components/Stages/prompt_ollama" >}}) — Enrich documents using a locally-running LLM.
- [QueryOpensearch]({{< relref "docs/architecture/components/Stages/query_opensearch" >}}) — Execute OpenSearch search templates per document.

## Conditions

For any Stage, you can specify `conditions` in its Config to control when the Stage processes a Document. Each
condition has a required parameter, `fields`, and two optional parameters, `operator` and `values`.

* `fields` — List of field names that will determine whether the Stage applies to a Document.
* `values` — List of values to search for in those fields. If not specified, only field *existence* is checked.
* `operator` — `"must"` (default) or `"must_not"`.

In the root of the Stage's Config, specify `conditionPolicy` — `"any"` (default) or `"all"` — to control whether
**any** or **all** conditions must be met for the Stage to execute.

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

## Plugin Stages

Additional stages are available as optional plugin modules. See [Plugins]({{< relref "docs/architecture/components/plugins" >}}).
