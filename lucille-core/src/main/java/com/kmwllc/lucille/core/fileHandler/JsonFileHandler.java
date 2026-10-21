package com.kmwllc.lucille.core.fileHandler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.DocumentException;
import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.util.FieldFilter;
import com.typesafe.config.Config;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.UnaryOperator;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.StringUtils;

/**
 * A file handler for JSON lines.
 * <p>
 * Reads each line as a JSON object and builds a {@link Document}.
 * <p>
 * <b>Note:</b> if your input JSON has its own "id" field but you've configured a different field for IDs, your original
 * "id" will be overwritten by the generated one in the documents.
 * <p>
 * A line that cannot be turned into a Document is logged and skipped, and the following lines are still processed. This
 * covers malformed JSON, JSON that is not an object, and a missing, empty or blank id. If 10 lines in a row cannot be read
 * the file fails, because the cause is then likely a wrong configuration or the wrong file format.
 */
public class JsonFileHandler extends BaseFileHandler {

  public static final Spec SPEC = SpecBuilder.fileHandler()
      .optionalString("docIdFormat", "idField")
      .optionalList("idFields", new TypeReference<List<String>>() {})
      .optionalList("blacklist", new TypeReference<List<String>>() {})
      .optionalList("whitelist", new TypeReference<List<String>>() {})
      .build();

  private static final Logger log = LoggerFactory.getLogger(JsonFileHandler.class);

  private final UnaryOperator<String> idUpdater;
  private final List<String> idFields;
  private final String docIdFormat;
  private final ObjectMapper mapper = new ObjectMapper();
  private final FieldFilter fieldFilter;

  public JsonFileHandler(Config config) {
    super(config);

    if (config.hasPath("idField")) {
      this.idFields = List.of(config.getString("idField"));
    } else {
      this.idFields = config.hasPath("idFields") ? config.getStringList("idFields") : List.of();
    }

    this.docIdFormat = config.hasPath("docIdFormat") ? config.getString("docIdFormat") : null;

    // returning null for a blank id makes fromJsonString reject the line, so it is skipped like any other bad record
    this.idUpdater = (id) -> Document.isValidId(id) ? docIdPrefix + id : null;

    this.fieldFilter = new FieldFilter(config);

    if (!fieldFilter.getBlacklist().isEmpty()) {
      Collection<String> intersection = CollectionUtils.intersection(this.idFields, fieldFilter.getBlacklist());
      if (!intersection.isEmpty()) {
        throw new IllegalArgumentException(
            "blacklist cannot contain idFields. Conflicting fields: " + intersection
        );
      }
    }

    if (!fieldFilter.getWhitelist().isEmpty()) {
      Collection<String> missing = CollectionUtils.subtract(this.idFields, fieldFilter.getWhitelist());
      if (!missing.isEmpty()) {
        throw new IllegalArgumentException(
            "whitelist must contain all idFields. Missing fields: " + missing
        );
      }
    }

    if (idFields.isEmpty()) {
      if (fieldFilter.getBlacklist().contains(Document.ID_FIELD)) {
        throw new IllegalArgumentException(
            "blacklist cannot contain \"" + Document.ID_FIELD + "\" when no idFields are configured."
        );
      }
      if (!fieldFilter.getWhitelist().isEmpty() && !fieldFilter.getWhitelist().contains(Document.ID_FIELD)) {
        throw new IllegalArgumentException(
            "whitelist must contain \"" + Document.ID_FIELD + "\" when no idFields are configured."
        );
      }
    }
  }

  @Override
  public Iterator<Document> processFile(InputStream inputStream, String path) throws FileHandlerException {
    // reader will be closed when the LineIterator is closed in getDocumentIterator
    Reader reader;

    try {
      reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
    } catch (Exception e) {
      throw new FileHandlerException(String.format("Error creating reader from file. (%s)", path), e);
    }

    return getDocumentIterator(reader, path);
  }

  private Iterator<Document> getDocumentIterator(Reader reader, String path) {
    return new Iterator<Document>() {
      // closing LineIterator closes reader, which closes BufferedReader, InputStreamReader and the InputStream passed into InputStreamReader
      private final LineIterator it = IOUtils.lineIterator(reader);
      private final BadRecordTracker tracker = new BadRecordTracker(path);
      private int lineNum = 0;
      // the next readable document, read ahead by hasNext() so that lines that cannot be read can be skipped
      private Document pending = null;
      private boolean finished = false;

      @Override
      public boolean hasNext() {
        if (pending != null) {
          return true;
        }
        if (finished) {
          return false;
        }

        try {
          while (it.hasNext()) {
            lineNum++;
            String line = it.next();

            if (StringUtils.isBlank(line)) {
              log.warn("Skipping blank line {}. ({})", lineNum, path);
              continue;
            }

            try {
              pending = createDocument(line, lineNum, path);
              tracker.good();
              return true;
            } catch (DocumentException | JsonProcessingException | IllegalArgumentException e) {
              // a problem with this line's data: skip it and carry on with the next line
              tracker.bad(lineNum, reasonFor(e));
            }
          }
        } catch (RuntimeException e) {
          // too many bad lines in a row, an I/O error reading the file, or an unexpected failure: the file cannot continue
          finished = true;
          IOUtils.closeQuietly(it);
          log.error("Error reading JSON line {}. ({})", lineNum, path, e);
          throw e;
        }

        // Iterator closes when reader is done reading (successful job)
        finished = true;
        IOUtils.closeQuietly(it);
        tracker.logSummary(lineNum);
        return false;
      }

      @Override
      public Document next() {
        // additional safety check to ensure that the iterator has more lines to process, if hasNext returns false,
        // means we have also closed LineIterator, throw an exception
        if (!hasNext()) {
          log.warn("No more lines to process. ({})", path);
          throw new NoSuchElementException(String.format("No more lines to process. (%s)", path));
        }

        Document doc = pending;
        pending = null;
        return doc;
      }
    };
  }

  /**
   * Builds a Document from one line. Throws DocumentException, JsonProcessingException or IllegalArgumentException (for
   * example from a docIdFormat that does not match the idFields) when the line's data cannot be turned into a Document.
   */
  private Document createDocument(String line, int lineNum, String path) throws DocumentException, JsonProcessingException {
    if (idFields.isEmpty()) {
      return JsonDocument.fromJsonString(line, idUpdater, fieldFilter);
    }

    JsonNode parsed = mapper.readTree(line);
    if (!parsed.isObject()) {
      throw new DocumentException("the line must be a JSON object but was " + JsonDocument.describe(parsed));
    }
    ObjectNode node = (ObjectNode) parsed;
    if (fieldFilter.isActive()) {
      Set<String> discardedFields = new HashSet<>();
      Iterator<String> iter = node.fieldNames();
      while (iter.hasNext()) {
        String fieldName = iter.next();
        if (!fieldFilter.shouldInclude(fieldName)) {
          discardedFields.add(fieldName);
        }
      }
      node.remove(discardedFields);
    }
    List<String> parts = new ArrayList<>(idFields.size());

    for (String fieldName : idFields) {
      JsonNode valueNode = node.get(fieldName);
      parts.add((valueNode != null && !valueNode.isNull()) ? valueNode.asText() : "");
      String value = (valueNode != null && !valueNode.isNull()) ? valueNode.asText() : null;
      if (StringUtils.isBlank(value)) {
        log.warn("Missing/blank idField {} at line {}. ({})", fieldName, lineNum, path);
      }
    }

    String rawId = (docIdFormat != null) ? String.format(docIdFormat, parts.toArray()) : String.join("_", parts);
    // an id with only some blank parts, such as "2_", is usable; one that is blank as a whole is not
    if (!Document.isValidId(rawId)) {
      throw new DocumentException("the id built from " + idFields + " is blank");
    }
    node.put(Document.ID_FIELD, docIdPrefix + rawId);

    return Document.create(node);
  }

  private static String reasonFor(Exception e) {
    String message = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
    return message.split("\n")[0];
  }
}
