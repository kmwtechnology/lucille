package com.kmwllc.lucille.stage;

import com.dashjoin.jsonata.JException;
import com.dashjoin.jsonata.Jsonata;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.DocumentException;
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
 *   <li>destination (String, Optional) : The destination field into which the json response should be placed. Defaults to mutating the source
 *   field. Has no effect if source is not specified.</li>
 *   <li>expression (String) : The Jsonata expression you want to apply to each Document.</li>
 * </ul>
 */
public class ApplyJSONata extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .requiredString("expression")
      .optionalString("source", "destination").build();

  private static final Logger log = LoggerFactory.getLogger(ApplyJSONata.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private final String source;
  private final String destination;
  private final String expressionStr;
  private Jsonata parsedExpression;

  public ApplyJSONata(Config config) throws StageException {
    super(config);
    this.source = ConfigUtils.getOrDefault(config, "source", null);
    this.destination = ConfigUtils.getOrDefault(config, "destination", null);
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
    if (source == null) {
      try {
        doc.transform(parsedExpression);
      } catch (DocumentException e) {
        log.warn("Exception occurred when applying transformation to document ({}). No change will take place.", doc.getId(), e);
      }
      return null;
    }

    if (!doc.has(source))  {
      log.info("Field {} not found on Document, Jsonata will not be applied.", source);
      return null;
    }

    try {
      JsonNode sourceNode = doc.getJson(source);
      Object output = parsedExpression.evaluate(toJsonataInput(sourceNode));

      if (output == null) {
        log.info("No jsonata output for document ({}). No change will take place.", doc.getId());
        return null;
      }

      // Converting the output back to a Jackson-compatible value.
      String destField = destination != null ? destination : source;
      doc.setField(destField, objectMapper.valueToTree(output));
    } catch(Exception e) {
      log.warn("Exception occurred when applying transformation to document ({}). No change will take place.", doc.getId(), e);
    }

    return null;
  }

  // recursively convert JsonNode to a Map/List/primitive for Jsonata to work with, including converting binary nodes to base64 strings
  private Object toJsonataInput(JsonNode node) {
    if (node.isObject()) {
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
    } else if (node.isNull()) {
      return null;
    } else {
      return node.toString();
    }
  }
}
