package com.kmwllc.lucille.stage;

import com.dashjoin.jsonata.JException;
import com.dashjoin.jsonata.Jsonata;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.core.DocumentException;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import static com.dashjoin.jsonata.Jsonata.jsonata;

/**
 * Applies a given Jsonata expression to extract information from a Document's field or to transform a Document entirely.
 * Applying a transformation to an entire document is an experimental feature and should be used with caution.
 * See <a href="https://github.com/dashjoin/jsonata-java">here</a> for Jsonata implementation.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>source (String, Optional) : The field to use for input. Defaults to applying your expression the entire Document and mutating the entire
 *   Document in place. Logs warning and skips document if this transformation fails, mutates reserved fields, or returns a non-object (primitive or array).</li>
 *   <li>dest (String, Optional) : The dest field into which the json response should be placed. Defaults to mutating the source
 *   field. Has no effect if source is not specified.</li>
 *   <li>expression (String) : The Jsonata expression you want to apply to each Document.</li>
 * </ul>
 */
public class ApplyJSONata extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .requiredString("expression")
      .optionalString("source", "dest").build();

  private static final Logger log = LoggerFactory.getLogger(ApplyJSONata.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String source;
  private final String destination;
  private final String expressionStr;
  private Jsonata parsedExpression;

  public ApplyJSONata(Config config) throws StageException {
    super(config);
    this.source = ConfigUtils.getStringOrDefault(config, "source", null);
    this.destination = ConfigUtils.getStringOrDefault(config, "dest", null);
    this.expressionStr = config.getString("expression");
  }

  @Override
  public void start() throws StageException {
    try {
      parsedExpression = jsonata(expressionStr);
    } catch (JException e) {
      throw new StageException("Exception occurred while parsing expression: " + e.getLocalizedMessage());
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    try {
      if (source == null) { // transform on whole doc
        transform(doc, parsedExpression);
      } else {
        if (!doc.has(source))  {
          log.info("Field {} not found on Document, Jsonata will not be applied.", source);
          return null;
        }
        transform(doc, parsedExpression, source, destination);
      }
    } catch(Exception e) {
      log.warn("Exception occurred when applying transformation to document ({}). No change will take place.", doc.getId(), e);
    }

    return null;
  }

  /**
   * Applies a JSONata expression to the Document (as if it were a JSON object) and replaces the Document's fields with
   * the result. The Document is left unchanged if this throws.
   *
   * @throws DocumentException if the result is not an object (primitive or array), or if it changes, removes or adds
   * a reserved field
   */
  static void transform(Document doc, Jsonata expr) throws DocumentException {
    ObjectNode docNode = MAPPER.createObjectNode();
    for (String name : doc.getFieldNames()) {
      docNode.set(name, doc.getJson(name));
    }

    Object transformed = expr.evaluate(toJsonataInput(docNode));

    if (transformed == null) {
      throw new DocumentException("Transformation must return a Map (JSON object), returned null");
    } else if (!(transformed instanceof Map)) {
      throw new DocumentException("Transformation must return a Map (JSON object), returned " + transformed.getClass());
    }

    // Jsonata-java outputs a Map, converting to ObjectNode so we can update the document.
    ObjectNode transformedNode = MAPPER.valueToTree(transformed);

    for (String reserved : Document.RESERVED_FIELDS) {
      if (!Objects.equals(docNode.get(reserved), transformedNode.get(reserved))) {
        throw new DocumentException("The given transformation mutates a reserved field");
      }
    }
    if (transformedNode.has("")) {
      throw new DocumentException("Transformation returned a field with an empty name");
    }

    for (String name : doc.getFieldNames()) {
      if (!Document.RESERVED_FIELDS.contains(name)) {
        doc.removeField(name);
      }
    }
    for (Iterator<Map.Entry<String, JsonNode>> it = transformedNode.fields(); it.hasNext(); ) {
      Map.Entry<String, JsonNode> field = it.next();
      if (!Document.RESERVED_FIELDS.contains(field.getKey())) {
        doc.setField(field.getKey(), field.getValue());
      }
    }
  }

  /**
   * Applies a JSONata expression to the value of the source field and places the result in the dest field, or
   * in the source field if destField is null.
   *
   * @throws DocumentException if the source field is absent or null, or the expression returns null
   */
  static void transform(Document doc, Jsonata expr, String sourceField, String destField) throws DocumentException {
    JsonNode sourceNode = doc.getNestedJson(sourceField);
    Object transformed = expr.evaluate(toJsonataInput(sourceNode));

    if (transformed == null) {
      throw new DocumentException("Transformation returned null");
    }

    // Converting the output back to a Jackson-compatible value.
    if (destField == null) {
      destField = sourceField;
    }
    doc.setNestedJson(destField, MAPPER.valueToTree(transformed));
  }

  /**
   * Recursively converts a JsonNode to a Map/List/primitive, including converting binary nodes to base64 strings
   * for compatibility with jsonata-java, which accepts only raw values, Maps, or Lists (not arrays or binary types).
   *
   * This is used in place of Document.asMap() for JSONata input: asMap() returns byte[] for binary nodes, which
   * jsonata-java cannot evaluate, whereas this returns the base64 text.
   *
   * @param node the JsonNode to convert
   * @return Object that can be evaluated by jsonata-java
   */
  private static Object toJsonataInput(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    } else if (node.isObject()) {
      Map<String, Object> map = new HashMap<>();
      node.fields().forEachRemaining(entry ->
          map.put(entry.getKey(), toJsonataInput(entry.getValue()))
      );
      return map;
    } else if (node.isArray()) {
      List<Object> list = new ArrayList<>();
      node.forEach(element -> list.add(toJsonataInput(element)));
      return list;
    } else if (node.isBinary()) {
      try {
        return Base64.getEncoder().encodeToString(node.binaryValue());
      } catch (IOException e) {
        return null;
      }
    } else if (node.isTextual()) {
      return node.asText();
    } else if (node.isNumber()) {
      return node.numberValue();
    } else if (node.isBoolean()) {
      return node.booleanValue();
    } else {
      return node.toString();
    }
  }
}
