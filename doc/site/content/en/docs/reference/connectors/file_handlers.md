---
title: File Handlers
date: 2025-02-28
description: File Handlers extract Lucille Documents from individual files, like CSV or JSON files, which themselves contain data which can be transformed into Lucille Documents.
---

File Handlers accept an `InputStream` for processing and return the Documents they extract in an `Iterator`. The provided `InputStream` and any underlying resources are closed when the Iterator returns `false` for `hasNext()`. When working directly with File Handlers in code, always exhaust the returned Iterator.

File Handlers are used with the `FileConnector` (via `fileHandlers`) and the `ApplyFileHandlers` Stage.

## Configuring File Handlers

File Handlers are configured under the `fileHandlers` block in a `FileConnector` config. The key (`csv`, `json`, `xml`) determines which handler is applied to files with that extension. To use a custom handler class, add a `class` field.

**Note:** `fileHandlers` and `fileOptions` are two separate config blocks. `fileHandlers` specifies which file types to process and how; `fileOptions` controls traversal behavior (`getFileContent`, `handleArchivedFiles`, `handleCompressedFiles`, `moveToAfterProcessing`, `moveToErrorFolder`).

```hocon
fileHandlers: {
  csv {
    separatorChar: "|"
    docIdPrefix: "csv-"
  }
  json {}
  xml {
    chunkPath: "//record"
  }
}
```

All File Handlers support `docIdPrefix` (inherited from the base FileHandler spec).

---

## CSV File Handler

`com.kmwllc.lucille.core.fileHandler.CSVFileHandler`

Extracts one Document per row from a CSV file.

### Configuration Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `idField` | String | — | Single column name whose value becomes the Document ID. |
| `idFields` | List\<String\> | — | Multiple column names combined to form the Document ID. |
| `docIdFormat` | String | — | Java `String.format` pattern for constructing the Document ID from column values. |
| `lineNumberField` | String | `csvLineNumber` | Field name for storing the row's line number. |
| `filenameField` | String | `filename` | Field name for storing the source filename. |
| `filePathField` | String | `source` | Field name for storing the full file path. |
| `separatorChar` | String | `,` | Column delimiter character. |
| `useTabs` | Boolean | `false` | Use tab as delimiter (overrides `separatorChar`). |
| `interpretQuotes` | Boolean | `true` | Treat `"` as a quoting character. |
| `ignoreEscapeChar` | Boolean | `false` | Disable backslash escape handling. |
| `lowercaseFields` | Boolean | `false` | Convert column header names to lowercase field names. |
| `ignoredTerms` | List\<String\> | — | Column values matching these strings are excluded from the document. |
| `docIdPrefix` | String | — | Prefix prepended to every Document ID. |

### Example

```hocon
fileHandlers: {
  csv {
    idField: "article_id"
    separatorChar: "|"
    filenameField: "source_file"
    lowercaseFields: true
    docIdPrefix: "article-"
  }
}
```

---

## JSON File Handler

`com.kmwllc.lucille.core.fileHandler.JSONFileHandler`

Extracts one Document per JSON object. Supports both standard JSON files (a single JSON object or array) and JSON Lines (`.jsonl`) format, where each line is a separate JSON object.

### Configuration Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `idField` | String | — | JSON field whose value becomes the Document ID. |
| `idFields` | List\<String\> | — | Multiple JSON fields combined to form the Document ID. |
| `docIdFormat` | String | — | Java `String.format` pattern for constructing the Document ID. |
| `blacklist` | List\<String\> | — | JSON fields to exclude from the Document. |
| `whitelist` | List\<String\> | — | Only include these JSON fields on the Document. |
| `docIdPrefix` | String | — | Prefix prepended to every Document ID. |

### Example

```hocon
fileHandlers: {
  json {
    idField: "doc_id"
    blacklist: ["internal_metadata", "_rev"]
    docIdPrefix: "doc-"
  }
}
```

---

## XML File Handler

`com.kmwllc.lucille.core.fileHandler.XMLFileHandler`

Extracts Documents from an XML file by selecting elements matching an XPath expression.

### Configuration Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `chunkPath` | String | Yes | XPath expression selecting the XML elements to convert into Documents (e.g., `"//record"`, `"/root/items/item"`). |
| `docIdPrefix` | String | No | Prefix prepended to every Document ID. |

Each matched element is converted to a Document. Element child text content and attributes become Document fields.

### Example

```hocon
fileHandlers: {
  xml {
    chunkPath: "//product"
  }
}
```

---

## Custom File Handlers

Developers can implement custom File Handlers for any file format.

1. Extend `BaseFileHandler`.
2. Declare a `public static final Spec SPEC = SpecBuilder.fileHandler()...` (required).
3. Implement `Iterator<Document> processFile(InputStream stream, String pathStr)`.

To use a custom handler, reference its `class` in the handler config:

```hocon
fileHandlers: {
  csv {
    class: "com.example.MyCustomCSVHandler"
    myCustomParam: "value"
  }
}
```

The `class` field can also override the default handler for a built-in file type.

---

## Archive and Compressed File Handling

The `FileConnector` can unpack archive files (zip, tar, tar.gz) and compressed files (gz) before applying file handlers. Enable this in `fileOptions` and declare the handlers to apply in `fileHandlers` (these are separate config blocks):

```hocon
fileOptions: {
  handleArchivedFiles: true
  handleCompressedFiles: true
}
fileHandlers: {
  csv { ... }
}
```

Files inside archives are referenced using the `!` separator: `archive.zip!path/to/file.csv`.
