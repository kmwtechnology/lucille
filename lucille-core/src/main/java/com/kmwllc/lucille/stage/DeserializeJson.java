package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class DeserializeJson extends Stage {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private final boolean includePrefix;

  private final String src;

  public DeserializeJson(Config config) {
    super(config, new StageSpec()
        .withRequiredProperties("src")
        .withOptionalProperties("includePrefix"));
    this.src = config.getString("src");
    this.includePrefix = ConfigUtils.getOrDefault(config, "includePrefix", false);
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {

    if (!doc.has(this.src)) {
      throw new StageException("Document does not have field: " + this.src);
    }

    JsonNode json = getJsonNode(doc.getString(this.src));
    doc.removeField(this.src);
    processJsonNode(doc, json, this.src);

    return null;
  }

  private static JsonNode getJsonNode(String content) throws StageException {
    try {
      return MAPPER.readTree(content);
    } catch (JsonProcessingException e) {
      throw new StageException("Unexpected error parsing JSON.", e);
    }
  }

  private void processJsonNode(Document doc, JsonNode json, String prefix) throws StageException {

    // only check if the outermost json is an object
    if (!json.isObject()) {
      throw new StageException("Expected JSON object, got: " + json);
    }

    Iterator<Map.Entry<String, JsonNode>> it = json.fields();
    while (it.hasNext()) {
      Entry<String, JsonNode> entry = it.next();
      String name = entry.getKey();
      JsonNode value = entry.getValue();

      String fieldName = getFieldName(doc, prefix, name);

      // todo consider checking if field is duplicate and has the same value

      if (value.isObject()) {
        // does not collapse nested objects
        doc.setField(fieldName, value);
      } else if (value.isArray()) {

        Iterator<JsonNode> elements = value.elements();
        while (elements.hasNext()) {
          JsonNode element = elements.next();
          doc.addToField(fieldName, element);
        }
      } else {
        doc.setField(fieldName, value.asText());
      }
    }
  }

  private String getFieldName(Document doc, String prefix, String name) {
    String fieldName = name;
    if (includePrefix) {
      fieldName = prefix + "_" + name;
    }

    // add a postfix if the field already exists
    int counter = 1;
    String postFix = "";
    while (doc.has(fieldName + postFix)) {
      postFix = "" + counter++;
    }
    return fieldName + postFix;
  }
}
