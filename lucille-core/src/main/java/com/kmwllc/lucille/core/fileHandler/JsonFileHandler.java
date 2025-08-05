package com.kmwllc.lucille.core.fileHandler;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.Spec.ParentSpec;
import com.typesafe.config.Config;
import dev.langchain4j.agent.tool.P;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.UnaryOperator;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonFileHandler extends BaseFileHandler {

  public static final Spec SPEC = Spec.fileHandler()
      .optionalString("docIdFormat", "idField")
      .optionalList("idFields", new TypeReference<List<String>>() {
      });

  private static final Logger log = LoggerFactory.getLogger(JsonFileHandler.class);

  private final UnaryOperator<String> idUpdater;
  private final List<String> idFields;
  private final String docIdFormat;
  private final ObjectMapper mapper = new ObjectMapper();

  public JsonFileHandler(Config config) {
    super(config);

    if (config.hasPath("idField")) {
      this.idFields = List.of(config.getString("idField"));
    } else {
      this.idFields = config.hasPath("idFields") ? config.getStringList("idFields") : List.of();
    }

    this.docIdFormat = config.hasPath("docIdFormat") ? config.getString("docIdFormat") : null;

    this.idUpdater = (id) -> docIdPrefix + id;
  }

  @Override
  public Iterator<Document> processFile(InputStream inputStream, String pathStr) throws FileHandlerException {
    // reader will be closed when the LineIterator is closed in getDocumentIterator
    Reader reader;

    try {
      reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
    } catch (Exception e) {
      throw new FileHandlerException("Error creating reader from file: " + pathStr, e);
    }

    return getDocumentIterator(reader);
  }

  private Iterator<Document> getDocumentIterator(Reader reader) {
    return new Iterator<Document>() {
      // closing LineIterator closes reader, which closes BufferedReader, InputStreamReader and the InputStream passed into InputStreamReader
      private final LineIterator it = IOUtils.lineIterator(reader);

      @Override
      public boolean hasNext() {
        boolean hasNext = it.hasNext();
        // Iterator closes when reader is done reading (successful job)
        if (!hasNext) {
          IOUtils.closeQuietly(it);
        }
        return hasNext;
      }

      @Override
      public Document next() {
        // additional safety check to ensure that the iterator has more lines to process, if hasNext returns false,
        // means we have also closed LineIterator, throw an exception
        if (!hasNext()) {
          throw new NoSuchElementException("No more lines to process");
        }

        String line = it.next();
        try {
          if (idFields.isEmpty()) {
            return Document.createFromJson(line, idUpdater);
          } else {
            ObjectNode node = (ObjectNode) mapper.readTree(line);
            List<String> parts = new ArrayList<>(idFields.size());

            for (String f : idFields) {
              JsonNode v = node.get(f);
              parts.add((v != null && !v.isNull()) ? v.asText() : "");
            }

            String rawId = (docIdFormat != null) ? String.format(docIdFormat, parts.toArray()) : String.join("_", parts);
            node.put(Document.ID_FIELD, docIdPrefix + rawId);

            return Document.create(node);
          }
        } catch (Exception e) {
          // any errors that occur during the process of creating a document, we close the LineIterator
          // cannot close iterator in finally, as we will called next() again if there are more elements.
          IOUtils.closeQuietly(it);
          throw new RuntimeException(
              "Error creating document, make sure that you have id field(s) properly configured within each line of json", e);
        }
      }
    };
  }
}
