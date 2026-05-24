---
title: Developing File Handlers
weight: 14
date: 2025-05-23
description: >
  How to implement a custom FileHandler for Lucille — parsing a new file format into Documents.
---

A FileHandler turns a file's content into Documents. Each implementation handles a specific file format — CSV, JSON, XML, or any custom format you need. The `FileConnector` uses FileHandlers to parse files it discovers during traversal.

To create a FileHandler, implement the `FileHandler` interface. The framework handles discovery, instantiation, and integration with the FileConnector.

**What the framework does for you:**

- **Discovery by file extension** — FileHandlers are mapped to file extensions in the connector's `fileHandlers` config block. The framework instantiates your handler and routes files to it based on their extension.
- **Config validation** — Your `getSpec()` method declares legal properties; the framework validates the config at startup.
- **InputStream management** — The framework opens the file (from local disk, S3, Azure, GCS, or inside archives) and passes you an `InputStream`. You don't need to know where the file came from.

**What you implement:**

| Method | Required | Purpose |
|---|---|---|
| `processFile(InputStream, String pathStr)` | Yes | Parse the stream and return an `Iterator<Document>`. The iterator should close resources when exhausted. |
| `processFileAndPublish(Publisher, InputStream, String)` | Yes | Parse and publish directly. For most handlers, this iterates `processFile()` and calls `publisher.publish()` for each document. |
| `getSpec()` | Yes | Return a `Spec` declaring your handler's legal config properties. |

**Constructor:** Your handler must have a public constructor that takes a single `Config` argument.

**What's in `config`:** The `Config` passed to your FileHandler constructor contains only the properties defined inside your handler's config block — the `{ ... }` value for your file extension key within the `fileHandlers` map. For example, if the user writes:

```hocon
fileHandlers: {
  yaml: {
    class: "com.mycompany.lucille.filehandler.YamlFileHandler"
    idField: "name"
  }
}
```

...your constructor receives a Config containing `class` and `idField`. It does not contain the connector config or the full Lucille config. Your SPEC should declare only your handler's own properties.

**Built-in defaults:** If the user configures `csv`, `json`, `jsonl`, or `xml` without specifying a `class`, Lucille uses its built-in handlers. Specifying a `class` for any extension (including the built-in ones) overrides the default.

---

## Referencing a Custom FileHandler from Config

Register your FileHandler by adding an entry to the `fileHandlers` block on a FileConnector, keyed by the file extension it handles. Include the `class` property with the fully qualified class name:

```hocon
connectors: [{
  name: "ingest-yaml"
  class: "com.kmwllc.lucille.connector.FileConnector"
  pipeline: "my-pipeline"
  paths: ["/data/configs/"]
  fileHandlers: {
    yaml: {
      class: "com.mycompany.lucille.filehandler.YamlFileHandler"
      idField: "name"
    }
    csv: {
      # no class needed — uses built-in CSVFileHandler
      separator: ","
    }
  }
}]
```

When the FileConnector encounters a file ending in `.yaml`, it passes its content to your `YamlFileHandler`. Files ending in `.csv` use the built-in handler. Files with extensions not listed in `fileHandlers` are skipped.

---

## Skeleton

Every file handler must follow the [Javadoc Standards]({{< relref "docs/developer-guide/javadocs" >}}).

```java
package com.mycompany.lucille.filehandler;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.fileHandler.FileHandler;
import com.kmwllc.lucille.core.fileHandler.FileHandlerException;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.yaml.snakeyaml.Yaml;

/**
 * Parses YAML files where each YAML document (separated by ---) becomes a Lucille Document.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>idField (String, Optional) : Field to use as the document ID. Defaults to generating an ID from the file path and index.</li>
 *   <li>docIdPrefix (String, Optional) : Prefix to prepend to document IDs. Defaults to empty string.</li>
 * </ul>
 */
public class YamlFileHandler implements FileHandler {

  public static final Spec SPEC = SpecBuilder.fileHandler()
      .optionalString("idField", "docIdPrefix")
      .build();

  private final String idField;
  private final String docIdPrefix;

  public YamlFileHandler(Config config) {
    this.idField = config.hasPath("idField") ? config.getString("idField") : null;
    this.docIdPrefix = config.hasPath("docIdPrefix") ? config.getString("docIdPrefix") : "";
  }

  @Override
  public Spec getSpec() {
    return SPEC;
  }

  @Override
  public Iterator<Document> processFile(InputStream inputStream, String pathStr)
      throws FileHandlerException {
    Yaml yaml = new Yaml();
    // loadAll returns an Iterable of parsed YAML documents (each as a Map)
    Iterator<Object> yamlDocs = yaml.loadAll(inputStream).iterator();

    return new Iterator<>() {
      private int index = 0;

      @Override
      public boolean hasNext() {
        return yamlDocs.hasNext();
      }

      @Override
      public Document next() {
        Object raw = yamlDocs.next();
        if (!(raw instanceof Map)) {
          throw new RuntimeException("YAML document at index " + index + " in " + pathStr
              + " is not a mapping");
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> fields = (Map<String, Object>) raw;

        // Determine document ID
        String id;
        if (idField != null && fields.containsKey(idField)) {
          id = docIdPrefix + fields.get(idField).toString();
        } else {
          id = docIdPrefix + pathStr + "-" + index;
        }

        Document doc = Document.create(id);
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
          doc.setField(entry.getKey(), entry.getValue().toString());
        }

        index++;
        return doc;
      }
    };
  }

  @Override
  public void processFileAndPublish(Publisher publisher, InputStream inputStream, String pathStr)
      throws FileHandlerException {
    Iterator<Document> docs = processFile(inputStream, pathStr);
    while (docs.hasNext()) {
      try {
        publisher.publish(docs.next());
      } catch (Exception e) {
        throw new FileHandlerException("Error publishing document from " + pathStr, e);
      }
    }
  }
}
```

---

## Guidelines

- **Return a lazy iterator** — Don't load the entire file into memory. Parse incrementally when the format supports it (YAML's `loadAll`, JSON streaming, etc.).
- **Close resources when exhausted** — If your iterator opens readers or parsers, close them when `hasNext()` returns false or when an exception is thrown.
- **Use `pathStr` for IDs and logging** — The path string identifies the file for error messages and can be used as part of document ID generation. Don't use it to open the file — the InputStream is already open.
- **Handle malformed input gracefully** — Throw `FileHandlerException` with a descriptive message rather than letting raw parsing exceptions propagate.
- **`processFileAndPublish` can usually delegate** — The pattern shown above (iterate `processFile()` and publish each document) works for most handlers. Override with a custom implementation only if you need to manage resources differently or publish in batches.

---

## Unit Testing

FileHandler tests follow a simple pattern: construct the handler with a config, open a test file as an InputStream, call `processFile()`, and assert on the returned documents.

```java
public class YamlFileHandlerTest {

  @Test
  public void testBasicParsing() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of("idField", "name"));
    YamlFileHandler handler = new YamlFileHandler(config);

    InputStream input = getClass().getClassLoader()
        .getResourceAsStream("YamlFileHandlerTest/sample.yaml");
    Iterator<Document> docs = handler.processFile(input, "sample.yaml");

    assertTrue(docs.hasNext());
    Document first = docs.next();
    assertEquals("my-service", first.getString("name"));
    assertEquals("8080", first.getString("port"));
  }
}
```

Place test YAML files under `src/test/resources/YamlFileHandlerTest/`.

For full pipeline integration tests, see [Testing Pipelines]({{< relref "docs/developer-guide/testing" >}}).
