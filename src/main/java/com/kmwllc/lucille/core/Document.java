package com.kmwllc.lucille.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * A record from a source system to be passed through a Pipeline, enriched,
 * and sent to a destination system.
 */
public class Document {

  public static final String ID_FIELD = "id";
  public static final String ERROR_FIELD = "errors";


  private final ObjectNode data;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public Document(ObjectNode data) throws DocumentException {

    if (!data.has(ID_FIELD)) {
      throw new DocumentException("id is missing");
    }

    JsonNode id = data.get(ID_FIELD);
    if (id.isNull() || id.asText().isEmpty()) {
      throw new DocumentException("id is present but null or empty");
    }

    this.data = data;
  }

  public Document(String id) throws DocumentException {
    if (id==null) {
      throw new DocumentException("ID cannot be null");
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

  public String getString(String name) {
    return data.get(name).isNull() ? null : data.get(name).asText();
  }

  public String getId() {
    return getString(ID_FIELD);
  }

  public boolean has(String name) {
    return data.has(name);
  }
}
