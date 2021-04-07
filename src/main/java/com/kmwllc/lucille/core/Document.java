package com.kmwllc.lucille.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.ConfigException;

import java.util.ArrayList;
import java.util.List;

/**
 * A record from a source system to be passed through a Pipeline, enriched,
 * and sent to a destination system.
 */
public class Document {

  public static final String ID_FIELD = "id";
  public static final String ERROR_FIELD = "errors";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final ObjectNode data;

  public Document(ObjectNode data) throws DocumentException {

    if (!data.hasNonNull(ID_FIELD)) {
      throw new DocumentException("id is missing");
    }

    JsonNode id = data.get(ID_FIELD);
    if (!id.isTextual() || id.asText().isEmpty()) {
      throw new DocumentException("id is present but null or empty or not a string");
    }

    this.data = data;
  }

  public Document(String id) {
    if (id==null) {
      throw new NullPointerException("ID cannot be null");
    }
    this.data = MAPPER.createObjectNode();
    this.data.put(ID_FIELD, id);
  }

  public static Document fromJsonString(String json) throws DocumentException, JsonProcessingException {
    return new Document((ObjectNode)MAPPER.readTree(json));
  }

  public void removeField(String name) {
    data.remove(name);
  }

  public void setField(String name, String value) {
    data.put(name, value);
  }

  public void setField(String name, Integer value) {
    data.put(name, value);
  }

  public void setField(String name, Boolean value) {
    data.put(name, value);
  }

  public String getString(String name) {
    return data.get(name).isNull() ? null : data.get(name).asText();
  }

  public List<String> getStringList(String name) {
    ArrayNode array = data.withArray(name);
    List<String> result = new ArrayList<>();
    for (JsonNode node : array) {
      result.add(node.asText());
    }
    return result;
  }

  public String getId() {
    return getString(ID_FIELD);
  }

  public boolean has(String name) {
    return data.has(name);
  }

  public boolean hasNonNull(String name) {
    return data.hasNonNull(name);
  }

  public boolean equals(Object other) {
    return data.equals(((Document)other).data);
  }

  private void convertToList(String name) {
    if (!data.has(name)) {
      data.set(name, MAPPER.createArrayNode());
      return;
    }
    JsonNode field = data.get(name);
    if (field.isArray()) {
      return;
    }
    ArrayNode array = MAPPER.createArrayNode();
    array.add(field);
    data.set(name, array);
  }

  public void addToField(String name, String value) {
    convertToList(name);
    ArrayNode array = data.withArray(name);
    array.add(value);
  }

  public void addToField(String name, Integer value) {
    convertToList(name);
    ArrayNode array = data.withArray(name);
    array.add(value);
  }

  public void addToField(String name, Boolean value) {
    convertToList(name);
    ArrayNode array = data.withArray(name);
    array.add(value);
  }

  public void logError(String description) {
    addToField(ERROR_FIELD, description);
  }

  @Override
  public String toString() {
    return data.toString();
  }

}
