---
title: Stages
weight: 4
date: 2025-06-06
description: Catalogue of built-in stages and how to configure them.
no_list: true
---

For conceptual documentation — what a Stage is, the Stage contract, conditions as a design decision, and child document emission — see [Architecture: Stage]({{< relref "docs/architecture/components/stage" >}}).

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

For the full reference on controlling document fate and connector sequencing — conditions, skipping, dropping, error handling, child documents, and more — see [Control Flow]({{< relref "docs/ingest-design/control-flow" >}}).

---

## Common Parameters

All stages share these common configuration parameters:

| Parameter | Description |
|---|---|
| `class` | **Required.** The fully qualified class name of the Stage. |
| `name` | Optional display name used in logging and metrics. |
| `conditions` | Optional list of conditions controlling when the Stage executes. |
| `conditionPolicy` | `"any"` or `"all"` (default: `"all"`). |

---

## Stage Catalogue

Every Stage has its own page, listed alphabetically in the navigation under **Stages**. The catalogue below groups them by purpose. Plugin stages that require an optional Maven module are marked accordingly.

### Field Manipulation

- [Concatenate]({{< relref "docs/ingest-design/stages/concatenate" >}}) — Concatenate the values of multiple source fields into a destination field.
- [CopyFields]({{< relref "docs/ingest-design/stages/copy_fields" >}}) — Copy one or more source fields to destination fields.
- [DeleteFields]({{< relref "docs/ingest-design/stages/delete_fields" >}}) — Remove specified fields from a Document.
- [DropValues]({{< relref "docs/ingest-design/stages/drop_values" >}}) — Remove specific values from multi-valued fields.
- [NormalizeFieldNames]({{< relref "docs/ingest-design/stages/normalize_field_names" >}}) — Normalize field names (lowercasing, replacing spaces, etc.).
- [RemoveDuplicateValues]({{< relref "docs/ingest-design/stages/remove_duplicate_values" >}}) — Remove duplicate values from multi-valued fields.
- [RemoveEmptyFields]({{< relref "docs/ingest-design/stages/remove_empty_fields" >}}) — Remove fields whose value is null, empty string, or empty list.
- [RenameFields]({{< relref "docs/ingest-design/stages/rename_fields" >}}) — Rename fields by mapping old names to new names.
- [SetStaticValues]({{< relref "docs/ingest-design/stages/set_static_values" >}}) — Set one or more fields to fixed, static values.
- [SplitFieldValues]({{< relref "docs/ingest-design/stages/split_field_values" >}}) — Split a field value by a delimiter into a list.

### Text Processing

- [ApplyRegex]({{< relref "docs/ingest-design/stages/apply_regex" >}}) — Apply a regular expression to extract capture groups or check for matches.
- [Base64Decode]({{< relref "docs/ingest-design/stages/base64_decode" >}}) — Decode a Base64-encoded field value.
- [CreateStaticTeaser]({{< relref "docs/ingest-design/stages/create_static_teaser" >}}) — Generate a teaser (short excerpt) from a text field.
- [ExtractFirstCharacter]({{< relref "docs/ingest-design/stages/extract_first_character" >}}) — Extract the first character of a string field.
- [HashFieldValueToBucket]({{< relref "docs/ingest-design/stages/hash_field_value_to_bucket" >}}) — Hash a field value and assign the document to a numbered bucket.
- [Length]({{< relref "docs/ingest-design/stages/length" >}}) — Compute the length of field values.
- [NormalizeText]({{< relref "docs/ingest-design/stages/normalize_text" >}}) — Apply Unicode normalization and optional case folding to text fields.
- [RemoveDiacritics]({{< relref "docs/ingest-design/stages/remove_diacritics" >}}) — Normalize accented characters to their ASCII equivalents.
- [ReplacePatterns]({{< relref "docs/ingest-design/stages/replace_patterns" >}}) — Perform pattern-based find-and-replace on string field values.
- [TrimWhitespace]({{< relref "docs/ingest-design/stages/trim_whitespace" >}}) — Trim leading and trailing whitespace from string fields.
- [TruncateField]({{< relref "docs/ingest-design/stages/truncate_field" >}}) — Truncate string field values to a maximum length.

### Type Conversion & Parsing

- [ParseDate]({{< relref "docs/ingest-design/stages/parse_date" >}}) — Parse date strings using configurable format patterns.
- [ParseFilePath]({{< relref "docs/ingest-design/stages/parse_file_path" >}}) — Extract path components from a file path field.
- [ParseFloats]({{< relref "docs/ingest-design/stages/parse_floats" >}}) — Parse a JSON array string into a list of floats.
- [ParseJson]({{< relref "docs/ingest-design/stages/parse_json" >}}) — Parse a JSON string field into a JsonNode.
- [Timestamp]({{< relref "docs/ingest-design/stages/timestamp" >}}) — Write the current timestamp to a field.

### Document Flow Control

- [CollapseChildrenDocuments]({{< relref "docs/ingest-design/stages/collapse_children_documents" >}}) — Copy field values from a Document's children onto the parent.
- [Contains]({{< relref "docs/ingest-design/stages/contains" >}}) — Check whether a field's value is contained in a configured list.
- [CreateChildrenStage]({{< relref "docs/ingest-design/stages/create_children_stage" >}}) — Generate child documents from the current document's fields.
- [DropDocument]({{< relref "docs/ingest-design/stages/drop_document" >}}) — Mark a document as dropped so it is not sent to the Indexer.
- [EmitNestedChildren]({{< relref "docs/ingest-design/stages/emit_nested_children" >}}) — Emit each element of a nested array as an independent child Document.
- [SkipDocument]({{< relref "docs/ingest-design/stages/skip_document" >}}) — Mark a document as skipped so it bypasses downstream Stages but still reaches the Indexer.

### HTML, XML, and Web

- [ApplyJSoup]({{< relref "docs/ingest-design/stages/apply_jsoup" >}}) — Parse HTML content and extract text or attributes using CSS selectors.
- [FetchUri]({{< relref "docs/ingest-design/stages/fetch_uri" >}}) — Fetch the content of a URL stored in a document field.
- [XPathExtractor]({{< relref "docs/ingest-design/stages/xpath_extractor" >}}) — Evaluate XPath expressions against an XML field.

### File Handling

- [ApplyFileHandlers]({{< relref "docs/ingest-design/stages/apply_file_handlers" >}}) — Apply configured FileHandlers to a byte array field, generating child documents.
- [ComputeFieldSize]({{< relref "docs/ingest-design/stages/compute_field_size" >}}) — Measure the byte size of a field value.
- [FetchFileContent]({{< relref "docs/ingest-design/stages/fetch_file_content" >}}) — Load the content of a file path into a byte array field.
- [TextExtractor]({{< relref "docs/ingest-design/stages/text_extractor" >}}) — Extract text from 1,000+ file formats using Apache Tika. *(requires `lucille-tika`)*
- [ApplyOCR]({{< relref "docs/ingest-design/stages/apply_ocr" >}}) — Perform OCR on image fields using Tesseract. *(requires `lucille-ocr`)*

### Enrichment & Lookup

- [DictionaryLookup]({{< relref "docs/ingest-design/stages/dictionary_lookup" >}}) — Look up field values in a term dictionary and add matched entries.
- [ElasticsearchLookup]({{< relref "docs/ingest-design/stages/elasticsearch_lookup" >}}) — Look up documents in an Elasticsearch index and merge matching fields.
- [MatchQuery]({{< relref "docs/ingest-design/stages/match_query" >}}) — Execute a match query against a search backend and enrich with results.
- [QueryDatabase]({{< relref "docs/ingest-design/stages/query_database" >}}) — Execute a JDBC prepared statement per document and merge the result.
- [QueryOpensearch]({{< relref "docs/ingest-design/stages/query_opensearch" >}}) — Execute an OpenSearch search template per document.

### AI / ML

- [ApplyJavascript]({{< relref "docs/ingest-design/stages/apply_javascript" >}}) — Run a JavaScript snippet per document using GraalVM.
- [ApplyJSONata]({{< relref "docs/ingest-design/stages/apply_jsonata" >}}) — Apply a JSONata expression to transform the document's JSON representation.
- [ChunkText]({{< relref "docs/ingest-design/stages/chunk_text" >}}) — Split long text fields into chunks for embedding and RAG pipelines.
- [DetectLanguage]({{< relref "docs/ingest-design/stages/detect_language" >}}) — Detect the language of a text field and write the ISO language code.
- [EmbeddedPython]({{< relref "docs/ingest-design/stages/embedded_python" >}}) — Run per-document Python code inside the JVM using GraalPy.
- [ExternalPython]({{< relref "docs/ingest-design/stages/external_python" >}}) — Delegate per-document processing to an external Python process via Py4J.
- [ExtractEntities]({{< relref "docs/ingest-design/stages/extract_entities" >}}) — Extract entities from text fields using configured rules.
- [ExtractEntitiesFST]({{< relref "docs/ingest-design/stages/extract_entities_fst" >}}) — Perform named entity recognition using a finite-state transducer dictionary.
- [OpenAIEmbed]({{< relref "docs/ingest-design/stages/openai_embed" >}}) — Generate vector embeddings using the OpenAI Embeddings API.
- [PromptOllama]({{< relref "docs/ingest-design/stages/prompt_ollama" >}}) — Send document fields to a locally-running Ollama LLM and merge the response.
- [RandomVector]({{< relref "docs/ingest-design/stages/random_vector" >}}) — Generate a random float vector (useful for testing vector search).
- [JlamaEmbed]({{< relref "docs/ingest-design/stages/jlama_embed" >}}) — Generate vector embeddings locally inside the JVM via Jlama. *(requires `lucille-jlama`)*
- [ApplyOpenNLPNameFinders]({{< relref "docs/ingest-design/stages/apply_opennlp_name_finders" >}}) — Perform named entity recognition using Apache OpenNLP models. *(requires `lucille-entity-extraction`)*

### Testing & Debugging

- [AddRandomBoolean]({{< relref "docs/ingest-design/stages/add_random_boolean" >}}) — Add a random boolean to a field.
- [AddRandomDate]({{< relref "docs/ingest-design/stages/add_random_date" >}}) — Add a random date/timestamp to a field within a range.
- [AddRandomDouble]({{< relref "docs/ingest-design/stages/add_random_double" >}}) — Add a random double to a field.
- [AddRandomInt]({{< relref "docs/ingest-design/stages/add_random_int" >}}) — Add a random integer to a field.
- [AddRandomNestedField]({{< relref "docs/ingest-design/stages/add_random_nested_field" >}}) — Add a nested JSON object with random values to a field.
- [AddRandomString]({{< relref "docs/ingest-design/stages/add_random_string" >}}) — Add a random alphanumeric string to a field.
- [Print]({{< relref "docs/ingest-design/stages/print" >}}) — Log documents as JSON and/or write them to a file (supports capture and replay).
