---
title: All Stages
weight: 1
date: 2025-06-09
description: A complete reference of all Stages available in lucille-core, organized by category.
---

This page lists all Stages available in `lucille-core` and as optional plugin modules.

All stages share common configuration parameters:

| Parameter | Description |
|---|---|
| `class` | **Required.** The fully qualified class name of the Stage. |
| `name` | Optional display name used in logging and metrics. |
| `conditions` | Optional list of conditions controlling when the Stage executes. |
| `conditionPolicy` | `"any"` or `"all"` (default: `"any"`). |

---

## Field Manipulation

### CopyFields
`com.kmwllc.lucille.stage.CopyFields`

Copies one or more source fields to destination fields.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | List\<String\> | Yes | Source field names. |
| `dest` | List\<String\> | Yes | Destination field names (parallel to `source`). |
| `updateMode` | String | No | `overwrite`, `append`, or `skip`. Default: `overwrite`. |

```hocon
{ class: "com.kmwllc.lucille.stage.CopyFields", source: ["title"], dest: ["title_copy"] }
```

---

### RenameFields
`com.kmwllc.lucille.stage.RenameFields`

Renames fields by mapping old names to new names.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fieldMapping` | Map\<String, String\> | Yes | Map of old field name → new field name. |

```hocon
{ class: "com.kmwllc.lucille.stage.RenameFields", fieldMapping: { old_field: new_field } }
```

---

### DeleteFields
`com.kmwllc.lucille.stage.DeleteFields`

Removes specified fields from a Document.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fields` | List\<String\> | Yes | Field names to delete. |

---

### SetStaticValues
`com.kmwllc.lucille.stage.SetStaticValues`

Sets one or more fields to fixed, static values.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fields` | Map\<String, Object\> | Yes | Map of field name → static value to set. |
| `updateMode` | String | No | `overwrite`, `append`, or `skip`. Default: `overwrite`. |
| `skipDocument` | Boolean | No | If `true`, marks the document as skipped after setting values. The document bypasses downstream stages but still reaches the Indexer. Useful for combining field-setting and skipping in a single stage with one set of conditions. Default: `false`. |

```hocon
{ class: "com.kmwllc.lucille.stage.SetStaticValues", fields: { source: "my-connector", version: 2 } }
```

---

### Concatenate
`com.kmwllc.lucille.stage.Concatenate`

Concatenates the values of multiple source fields into a destination field.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | List\<String\> | Yes | Fields whose values will be concatenated. |
| `dest` | String | Yes | Destination field name. |
| `delimiter` | String | No | Separator inserted between values. Default: `""`. |
| `updateMode` | String | No | `overwrite`, `append`, or `skip`. |

---

### SplitFieldValues
`com.kmwllc.lucille.stage.SplitFieldValues`

Splits a field value (or multi-valued field) by a delimiter into a list.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fields` | List\<String\> | Yes | Fields to split. |
| `delimiter` | String | No | Delimiter string. Default: `","`. |
| `updateMode` | String | No | `overwrite`, `append`, or `skip`. |

---

### RemoveDuplicateValues
`com.kmwllc.lucille.stage.RemoveDuplicateValues`

Removes duplicate values from multi-valued fields.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fields` | List\<String\> | Yes | Fields to deduplicate. |

---

### RemoveEmptyFields
`com.kmwllc.lucille.stage.RemoveEmptyFields`

Removes fields whose value is `null`, an empty string, or an empty list.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fields` | List\<String\> | No | Specific fields to check. If omitted, all fields are checked. |

---

### NormalizeFieldNames
`com.kmwllc.lucille.stage.NormalizeFieldNames`

Normalizes field names (e.g., lowercasing, replacing spaces with underscores).

---

### DropValues
`com.kmwllc.lucille.stage.DropValues`

Removes specific values from multi-valued fields.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fieldValuePairs` | Map\<String, List\<String\>\> | Yes | Map of field name → list of values to remove. |

---

## Text Processing

### TrimWhitespace
`com.kmwllc.lucille.stage.TrimWhitespace`

Trims leading and trailing whitespace from string fields.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fields` | List\<String\> | Yes | Fields to trim. |

---

### RemoveDiacritics
`com.kmwllc.lucille.stage.RemoveDiacritics`

Normalizes accented characters to their ASCII equivalents (e.g., `é` → `e`).

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fields` | List\<String\> | Yes | Fields to normalize. |

---

### TruncateField
`com.kmwllc.lucille.stage.TruncateField`

Truncates string field values to a maximum length.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fields` | List\<String\> | Yes | Fields to truncate. |
| `maxLength` | Integer | Yes | Maximum allowed length. |

---

### Length
`com.kmwllc.lucille.stage.Length`

Computes the length (number of characters or list elements) of field values and stores the result.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | List\<String\> | Yes | Fields to measure. |
| `dest` | List\<String\> | Yes | Fields to write lengths to. |

---

### ApplyRegex
`com.kmwllc.lucille.stage.ApplyRegex`

Applies a regular expression to one or more fields. Can extract capture groups or check for matches.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | List\<String\> | Yes | Fields to apply the regex to. |
| `dest` | List\<String\> | No | Fields to write extracted groups to (parallel to `source`). |
| `regex` | String | Yes | The regular expression pattern. |
| `updateMode` | String | No | `overwrite`, `append`, or `skip`. |

---

### ReplacePatterns
`com.kmwllc.lucille.stage.ReplacePatterns`

Performs pattern-based find-and-replace on string field values.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fields` | List\<String\> | Yes | Fields to process. |
| `patterns` | Map\<String, String\> | Yes | Map of regex pattern → replacement string. |

---

### Base64Decode
`com.kmwllc.lucille.stage.Base64Decode`

Decodes a Base64-encoded field value into a byte array or string.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | String | Yes | Field containing the Base64-encoded value. |
| `dest` | String | Yes | Field to write the decoded value to. |

---

### NormalizeText
`com.kmwllc.lucille.stage.NormalizeText`

Applies Unicode normalization and optional case folding to text fields.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fields` | List\<String\> | Yes | Fields to normalize. |

---

### ExtractFirstCharacter
`com.kmwllc.lucille.stage.ExtractFirstCharacter`

Extracts the first character of a string field.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | String | Yes | Source field. |
| `dest` | String | Yes | Destination field. |

---

### CreateStaticTeaser
`com.kmwllc.lucille.stage.CreateStaticTeaser`

Generates a teaser (short excerpt) from a text field.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | String | Yes | Source text field. |
| `dest` | String | Yes | Destination teaser field. |
| `length` | Integer | No | Maximum teaser length in characters. |

---

### HashFieldValueToBucket
`com.kmwllc.lucille.stage.HashFieldValueToBucket`

Hashes a field's value and assigns the document to a numbered bucket (useful for deterministic partitioning).

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | String | Yes | Field whose value is hashed. |
| `dest` | String | Yes | Field to write the bucket number to. |
| `numBuckets` | Integer | Yes | Total number of buckets. |

---

## Type Conversion & Parsing

### ParseDate
`com.kmwllc.lucille.stage.ParseDate`

Parses date strings using configurable format patterns and writes an `Instant` or formatted string.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | List\<String\> | Yes | Fields containing date strings. |
| `dest` | List\<String\> | No | Destination fields. Defaults to overwriting source. |
| `formats` | List\<String\> | Yes | Date format patterns to try, in order. |
| `timezone` | String | No | Timezone for parsing. Default: UTC. |

---

### ParseFloats
`com.kmwllc.lucille.stage.ParseFloats`

Parses a JSON array string (e.g., `"[0.1, 0.2, 0.3]"`) into a list of floats.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | String | Yes | Field containing the JSON array string. |
| `dest` | String | Yes | Destination field for the parsed list. |

---

### ParseFilePath
`com.kmwllc.lucille.stage.ParseFilePath`

Extracts path components (directory, filename, extension) from a file path field.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | String | Yes | Field containing the file path. |
| `directoryDest` | String | No | Destination for the directory component. |
| `filenameDest` | String | No | Destination for the filename (without extension). |
| `extensionDest` | String | No | Destination for the file extension. |

---

### ParseJson
`com.kmwllc.lucille.stage.ParseJson`

Parses a JSON string field into a `JsonNode`.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | String | Yes | Field containing a JSON string. |
| `dest` | String | Yes | Destination field for the parsed `JsonNode`. |

---

### Timestamp
`com.kmwllc.lucille.stage.Timestamp`

Writes the current timestamp to a field.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `dest` | String | No | Destination field. Default: `timestamp`. |

---

## Document Flow Control

### DropDocument
`com.kmwllc.lucille.stage.DropDocument`

Marks a document as dropped. It will not be sent to the Indexer.

Typically used with `conditions` to selectively drop documents:

```hocon
{
  class: "com.kmwllc.lucille.stage.DropDocument"
  conditions: [
    { fields: ["status"], values: ["deleted"] }
  ]
}
```

---

### SkipDocument
`com.kmwllc.lucille.stage.SkipDocument`

Marks a document as skipped. It bypasses all downstream Stages but still reaches the Indexer. Used to issue deletes against a search backend.

```hocon
{
  class: "com.kmwllc.lucille.stage.SkipDocument"
  conditions: [
    { fields: ["is_deleted"], values: ["true"] }
  ]
}
```

---

### Contains
`com.kmwllc.lucille.stage.Contains`

Checks whether a field's value is contained in a configured list. Sets a boolean result field.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `field` | String | Yes | Field to check. |
| `values` | List\<String\> | Yes | Values to look for. |
| `dest` | String | Yes | Destination boolean field. |

---

### EmitNestedChildren
`com.kmwllc.lucille.stage.EmitNestedChildren`

Extracts a nested array from a Document and emits each array element as an independent child Document.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `field` | String | Yes | Field containing the array of nested objects to extract. |
| `keepParent` | Boolean | No | Whether to also emit the parent Document. Default: `true`. |

---

### CreateChildrenStage
`com.kmwllc.lucille.stage.CreateChildrenStage`

Generates child documents from the current document's fields using configurable rules.

---

### CollapseChildrenDocuments
`com.kmwllc.lucille.stage.CollapseChildrenDocuments`

Merges child document field values back onto the parent document.

---

## HTML, XML, and Web

### ApplyJSoup
`com.kmwllc.lucille.stage.ApplyJSoup`

Parses HTML content using [JSoup](https://jsoup.org/) and extracts text or attribute values using CSS selectors.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `byteArrayField` | String | Yes | Field containing the HTML as a byte array. |
| `destinationFields` | Map | Yes | Map of destination field name → `{type, selector}` definition. |

Each destination field definition requires:
- `type`: `"text"` (inner text) or `"attr"` (attribute value).
- `selector`: CSS selector.
- `attr`: (if `type` is `"attr"`) the attribute name to extract.

```hocon
{
  class: "com.kmwllc.lucille.stage.ApplyJSoup"
  byteArrayField: "html_content"
  destinationFields: {
    title: { type: "text", selector: "h1" }
    body:  { type: "text", selector: ".article-body p" }
  }
}
```

---

### XPathExtractor
`com.kmwllc.lucille.stage.XPathExtractor`

Evaluates XPath expressions against an XML field.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `xmlField` | String | Yes | Field containing XML content. |
| `fieldMappings` | Map\<String, String\> | Yes | Map of destination field → XPath expression. |

---

### FetchUri
`com.kmwllc.lucille.stage.FetchUri`

Fetches the content of a URL stored in a document field and stores the response as a byte array.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | String | Yes | Field containing the URL to fetch. |
| `dest` | String | Yes | Field to write the response bytes to. |

---

## File Handling

### ApplyFileHandlers
`com.kmwllc.lucille.stage.ApplyFileHandlers`

Applies configured FileHandlers to a byte array field, generating child documents for each extracted record.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fileContentField` | String | No | Field containing the file bytes. Default: `file_content`. |
| `filePathField` | String | No | Field containing the file path (used to determine handler). Default: `file_path`. |
| `fileHandlers` | Object | **Yes** | FileHandler configuration — same structure as the `fileHandlers` block in `FileConnector`. At least one handler must be declared. |

---

### FetchFileContent
`com.kmwllc.lucille.stage.FetchFileContent`

Loads the content of a file path (from local filesystem or cloud storage) into a byte array field on the document.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `pathField` | String | Yes | Field containing the file path or URI. |
| `destField` | String | No | Destination byte array field. Default: `file_content`. |

---

### ComputeFieldSize
`com.kmwllc.lucille.stage.ComputeFieldSize`

Measures the byte size of a field value (e.g., a byte array or string) and stores the result.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | String | Yes | Field to measure. |
| `dest` | String | Yes | Destination field for the size in bytes. |

---

### TextExtractor

`com.kmwllc.lucille.tika.stage.TextExtractor` *(requires `lucille-tika`)*

Extracts text from over 1,000 file formats — PDF, Microsoft Office documents, HTML, images with embedded OCR, and many more — using [Apache Tika](https://tika.apache.org/). Reads raw bytes from a source field and writes extracted text to a destination field.

**Maven dependency:**
```xml
<dependency>
  <groupId>com.kmwllc</groupId>
  <artifactId>lucille-tika</artifactId>
  <version>${lucille.version}</version>
</dependency>
```

```hocon
{
  name: "extract-text"
  class: "com.kmwllc.lucille.tika.stage.TextExtractor"
  source: "file_content"
  dest: "extracted_text"
}
```

---

### ApplyOCR

`com.kmwllc.lucille.ocr.stage.ApplyOCR` *(requires `lucille-ocr`)*

Performs optical character recognition on image fields using [Tesseract](https://tesseract-ocr.github.io/). Reads image bytes from a source field and writes the recognized text to a destination field. Requires Tesseract to be installed on the system running Lucille.

**Maven dependency:**
```xml
<dependency>
  <groupId>com.kmwllc</groupId>
  <artifactId>lucille-ocr</artifactId>
  <version>${lucille.version}</version>
</dependency>
```

```hocon
{
  name: "ocr"
  class: "com.kmwllc.lucille.ocr.stage.ApplyOCR"
  source: "image_bytes"
  dest: "ocr_text"
  language: "eng"
}
```

---

## Enrichment & Lookup

### DictionaryLookup
`com.kmwllc.lucille.stage.DictionaryLookup`

Looks up field values in a term dictionary and adds matched entries as field values.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | List\<String\> | Yes | Fields whose values are used as lookup keys. |
| `dest` | List\<String\> | Yes | Destination fields for lookup results. |
| `dictPath` | String | Yes | Path to the dictionary file. |

---

### QueryDatabase
`com.kmwllc.lucille.stage.QueryDatabase`

Executes a JDBC prepared statement using document field values as parameters and merges the result onto the document. Useful for per-document database enrichment (e.g., joining a lookup table for each document mid-pipeline).

| Parameter | Type | Required | Description |
|---|---|---|---|
| `driver` | String | Yes | JDBC driver class name. |
| `connectionString` | String | Yes | JDBC connection URL. |
| `jdbcUser` | String | Yes | Database username. |
| `jdbcPassword` | String | Yes | Database password. |
| `sql` | String | No | SQL query with `?` placeholders for parameters. |
| `keyFields` | List\<String\> | Yes | Document field names whose values are substituted for `?` in the SQL, in order. |
| `inputTypes` | List\<String\> | Yes | JDBC types for each key field (e.g., `"STRING"`, `"INT"`, `"LONG"`). Must match `keyFields` length. |
| `fieldMapping` | Map\<String, String\> | Yes | Maps result-set column names to document field names. |

---

### ElasticsearchLookup
`com.kmwllc.lucille.stage.ElasticsearchLookup`

Performs a lookup against an Elasticsearch index and merges matching fields onto the document.

---

### QueryOpensearch
`com.kmwllc.lucille.stage.QueryOpensearch`

Executes a search template against an OpenSearch index using document field values as parameters. See [QueryOpensearch]({{< relref "docs/reference/stages/query_opensearch" >}}) for full documentation.

---

### MatchQuery
`com.kmwllc.lucille.stage.MatchQuery`

Executes a match query against a configured search backend and enriches the document with matching results.

---

## AI / ML

### OpenAIEmbed
`com.kmwllc.lucille.stage.OpenAIEmbed`

Generates vector embeddings for a text field using the OpenAI Embeddings API.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | String | **Yes** | Field containing the text to embed. |
| `apiKey` | String | **Yes** | OpenAI API key. Use `${OPENAI_API_KEY}` for environment variable substitution. |
| `embedDocument` | Boolean | **Yes** | Whether to embed the main document. |
| `embedChildren` | Boolean | **Yes** | Whether to embed child documents. |
| `dest` | String | No | Field to write the embedding vector to. Default: `embeddings`. |
| `modelName` | String | No | OpenAI embedding model. Default: `text-embedding-3-small`. |
| `dimensions` | Integer | No | Output vector dimensions (only supported by `text-embedding-3-*` models). |

**Supported models:** `text-embedding-3-small`, `text-embedding-3-large`, `text-embedding-ada-002`

Text is truncated to 8,191 tokens before embedding (the OpenAI API limit). Lucille uses [jtokkit](https://github.com/knuddelsgmbh/jtokkit) for accurate token counting before the API call.

```hocon
{
  class: "com.kmwllc.lucille.stage.OpenAIEmbed"
  source: "content"
  dest: "content_vector"
  modelName: "text-embedding-3-small"
  apiKey: ${OPENAI_API_KEY}
  embedDocument: true
  embedChildren: true
}
```

---

### JlamaEmbed

`com.kmwllc.lucille.jlama.stage.JlamaEmbed` *(requires `lucille-jlama`)*

Generates vector embeddings using a quantized LLM running locally inside the JVM via [Jlama](https://github.com/tjake/Jlama). No API key or external service required — the model runs directly in the Lucille process. Useful for teams with data-residency or compliance constraints that prevent sending documents to external APIs.

**Maven dependency:**
```xml
<dependency>
  <groupId>com.kmwllc</groupId>
  <artifactId>lucille-jlama</artifactId>
  <version>${lucille.version}</version>
</dependency>
```

```hocon
{
  name: "embed"
  class: "com.kmwllc.lucille.jlama.stage.JlamaEmbed"
  source: "content"
  dest: "content_vector"
  modelPath: "/models/my-embedding-model"
}
```

---

### PromptOllama
`com.kmwllc.lucille.stage.PromptOllama`

Sends document fields to a locally-running Ollama LLM and merges the JSON response back onto the document. See [PromptOllama]({{< relref "docs/reference/stages/prompt_ollama" >}}) for full documentation.

---

### EmbeddedPython
`com.kmwllc.lucille.stage.EmbeddedPython`

Runs per-document Python code inside the JVM using GraalPy. See [EmbeddedPython]({{< relref "docs/reference/stages/embedded_python" >}}) for full documentation.

---

### ExternalPython
`com.kmwllc.lucille.stage.ExternalPython`

Delegates per-document processing to an external Python process via Py4J. See [ExternalPython]({{< relref "docs/reference/stages/external_python" >}}) for full documentation.

---

### ApplyJavascript
`com.kmwllc.lucille.stage.ApplyJavascript`

Runs a JavaScript snippet per document using GraalVM's JavaScript engine.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `script` | String | No | Inline JavaScript code. |
| `scriptPath` | String | No | Path to a `.js` file. Exactly one of `script` or `scriptPath` must be provided. |

---

### ApplyJSONata
`com.kmwllc.lucille.stage.ApplyJSONata`

Applies a [JSONata](https://jsonata.org/) expression to transform the document's JSON representation.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `expression` | String | Yes | JSONata expression to apply. |
| `dest` | String | No | Destination field for the expression result. If omitted, results are merged onto the document. |

---

### ExtractEntitiesFST
`com.kmwllc.lucille.stage.ExtractEntitiesFST`

Performs named entity recognition using a finite-state transducer dictionary.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | String | Yes | Field containing text to extract entities from. |
| `dest` | String | Yes | Destination field for extracted entity values. |
| `dictionaryPath` | String | Yes | Path to the FST dictionary file. |

---

### ExtractEntities
`com.kmwllc.lucille.stage.ExtractEntities`

Extracts entities from text fields using configured rules.

---

### ApplyOpenNLPNameFinders *(requires lucille-entity-extraction)*
`com.kmwllc.lucille.entity.stage.ApplyOpenNLPNameFinders`

Performs named entity recognition (NER) using [Apache OpenNLP](https://opennlp.apache.org/) models. Identifies entities such as people, organizations, and locations in text fields.

**Maven dependency:**
```xml
<dependency>
  <groupId>com.kmwllc</groupId>
  <artifactId>lucille-entity-extraction</artifactId>
  <version>${lucille.version}</version>
</dependency>
```

---

### DetectLanguage
`com.kmwllc.lucille.stage.DetectLanguage`

Detects the language of a text field and writes the ISO language code to a destination field.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | String | Yes | Field containing the text. |
| `dest` | String | Yes | Destination field for the language code (e.g., `"en"`, `"fr"`). |

---

### ChunkText
`com.kmwllc.lucille.stage.ChunkText`

Splits a long text field into smaller chunks suitable for embedding (RAG pipelines). Each chunk is emitted as a **child document**. See [ChunkText]({{< relref "docs/reference/stages/chunk_text" >}}) for full documentation.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source` | String | **Yes** | Field containing the text to chunk. |
| `dest` | String | No | Field name for chunk content in child documents. Default: `text`. |
| `chunkingMethod` | String | No | Strategy: `sentence`, `paragraph`, `fixed`, or `custom`. Default: `sentence`. |
| `regex` | String | No* | Regex delimiter for `custom` chunking method. |
| `lengthToSplit` | Integer | No* | Characters per chunk for `fixed` chunking method. |
| `chunksToMerge` | Integer | No | Merge N initial chunks into one final chunk. Default: `1`. |
| `chunksToOverlap` | Integer | No | Number of chunks to overlap when merging. |
| `overlapPercentage` | Integer | No | Percentage of neighbouring chunks to add as overlap. Default: `0`. |
| `characterLimit` | Integer | No | Hard maximum character count for a final chunk. |
| `preMergeMinChunkLen` | Integer | No | Append chunks shorter than this to a neighbour before merging. |
| `preMergeMaxChunkLen` | Integer | No | Truncate chunks longer than this before merging. |
| `cleanChunks` | Boolean | No | Remove newlines and trim chunks. Default: `false`. |

**Chunking methods:**
- `sentence` — Detects sentence boundaries using OpenNLP.
- `paragraph` — Splits on consecutive line breaks (`\n\n`, `\r\n\r\n`, etc.).
- `fixed` — Splits every `lengthToSplit` characters.
- `custom` — Splits on occurrences of the `regex` pattern.

**Child document fields:** Each child document receives `id` (parent ID + chunk number), `parent_id`, `offset`, `length`, `chunk_number`, `total_chunks`, and the chunk content in `dest`.

---

### RandomVector
`com.kmwllc.lucille.stage.RandomVector`

Generates a random float vector and sets it on a document field. Useful for testing vector search pipelines.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `dest` | String | Yes | Destination field for the random vector. |
| `dimensions` | Integer | Yes | Number of dimensions in the vector. |

---

## Testing & Debugging

### Print
`com.kmwllc.lucille.stage.Print`

Logs documents in JSON format at INFO level and/or writes them to a file. Place `Print` anywhere in the pipeline to capture the document state at that point.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `shouldLog` | Boolean | No | Log each document as JSON at INFO level. Default: `true`. |
| `outputFile` | String | No | Path to a file to write documents to (one JSON object per line). Created if it does not exist. |
| `whitelist` | List\<String\> | No | If set, only these fields are included in the output. |
| `blacklist` | List\<String\> | No | Fields to exclude from the output. |
| `overwriteFile` | Boolean | No | Overwrite the output file if it already exists. Default: `true`. |
| `appendThreadName` | Boolean | No | Append the Worker thread name to the output filename, keeping per-thread output separate. Recommended when using multiple worker threads. Default: `true`. |

```hocon
{
  class: "com.kmwllc.lucille.stage.Print"
  shouldLog: false
  outputFile: "/tmp/pipeline-output.jsonl"
}
```

#### Capture and Replay

`Print` enables a useful development pattern: run a pipeline to capture its output to disk, then replay that output directly into a search backend without re-running the enrichment.

**Step 1 — Capture.** Add `Print` to the end of your pipeline with an `outputFile`. Use a `NopIndexer` (or set `sendEnabled: false` on a real indexer) so no data is actually indexed during the capture run:

```hocon
pipelines: [{
  name: my-pipeline
  stages: [
    { class: "com.kmwllc.lucille.stage.SomeExpensiveEnrichment" ... }
    {
      class: "com.kmwllc.lucille.stage.Print"
      shouldLog: false
      outputFile: "/tmp/captured.jsonl"
    }
  ]
}]
indexer { type: Nop }
```

**Step 2 — Replay.** Point a `FileConnector` at the captured JSONL file using the JSON file handler. Use a minimal or empty pipeline — the captured documents already contain all enriched fields:

```hocon
connectors: [{
  name: replay
  class: "com.kmwllc.lucille.connector.FileConnector"
  pipeline: replay-pipeline
  paths: ["/tmp/captured.jsonl"]
  fileHandlers: { json: { idField: "id" } }
}]
pipelines: [{
  name: replay-pipeline
  stages: []
}]
indexer { type: OpenSearch }
opensearch { ... }
```

This lets you iterate on indexer configuration, field mappings, or search backend settings without repeating expensive enrichment (OCR, embedding generation, database lookups) on every attempt.

---

### AddRandomString
`com.kmwllc.lucille.stage.AddRandomString`

Adds a random alphanumeric string to a field.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `field_name` | String | No | Destination field. Default: `random_string`. |
| `length` | Integer | No | String length. Default: `8`. |

---

### AddRandomInt
`com.kmwllc.lucille.stage.AddRandomInt`

Adds a random integer to a field.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `field_name` | String | No | Destination field. Default: `random_int`. |
| `min` | Integer | No | Minimum value (inclusive). Default: `0`. |
| `max` | Integer | No | Maximum value (exclusive). Default: `100`. |

---

### AddRandomDouble
`com.kmwllc.lucille.stage.AddRandomDouble`

Adds a random double to a field.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `field_name` | String | No | Destination field. Default: `random_double`. |
| `min` | Double | No | Minimum value. Default: `0.0`. |
| `max` | Double | No | Maximum value. Default: `1.0`. |

---

### AddRandomDate
`com.kmwllc.lucille.stage.AddRandomDate`

Adds a random date/timestamp to a field within a configured range.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `field_name` | String | No | Destination field. Default: `random_date`. |

---

### AddRandomBoolean
`com.kmwllc.lucille.stage.AddRandomBoolean`

Adds a random boolean to a field.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `field_name` | String | No | Destination field. Default: `random_bool`. |
| `percent_true` | Integer | No | Percentage chance of `true`. Default: `50`. |

---

### AddRandomNestedField
`com.kmwllc.lucille.stage.AddRandomNestedField`

Adds a nested JSON object with random values to a field. Useful for testing nested document structures.

---

