package com.kmwllc.lucille.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

/**
 * A record from a source system to be passed through a Pipeline, enriched,
 * and sent to a destination system.
 *
 */
public class Document implements Cloneable {

  public static final String ID_FIELD = "id";
  private static final String RUNID_FIELD = "run_id";
  public static final String ERROR_FIELD = "errors";
  public static final String CHILDREN_FIELD = ".children";

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TypeReference<Map<String, Object>> TYPE = new TypeReference<Map<String, Object>>(){};
  private static final Logger log = LoggerFactory.getLogger(Document.class);

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

  public Document(String id, String runId) {
    this(id);
    this.data.put(RUNID_FIELD, runId);
  }

  public static Document fromJsonString(String json) throws DocumentException, JsonProcessingException {
    return new Document((ObjectNode)MAPPER.readTree(json));
  }

  public void removeField(String name) {
    data.remove(name);
  }

  public void removeFromArray(String name, int index) {
    data.withArray(name).remove(index);
  }

  /**
   * Updates the designated field according to the provided UpdateMode.

   * APPEND: the provided values will be appended to the field.
   * OVERWRITE: the provided values will overwrite any current field values
   * SKIP: the provided values will populate the field if the field didn't previously exist; otherwise no change will be made.
   *
   * In all cases the field will be created if it doesn't already exist.
   *
   */
  public void update(String name, UpdateMode mode, String... values) {
    update(name, mode, (v)->{setField(name,(String)v);}, (v)->{addToField(name,(String)v);}, values);
  }

  public void update(String name, UpdateMode mode, Long... values) {
    update(name, mode, (v)->{setField(name,(Long)v);}, (v)->{addToField(name,(Long)v);}, values);
  }

  public void update(String name, UpdateMode mode, Integer... values) {
    update(name, mode, (v)->{setField(name,(Integer)v);}, (v)->{addToField(name,(Integer)v);}, values);
  }

  public void update(String name, UpdateMode mode, Boolean... values) {
    update(name, mode, (v)->{setField(name,(Boolean)v);}, (v)->{addToField(name,(Boolean)v);}, values);
  }

  public void update(String name, UpdateMode mode, Double... values) {
    update(name, mode, (v)->{setField(name,(Double)v);}, (v)->{addToField(name,(Double)v);}, values);
  }

  /**
   * Private helper method used by different public versions of the overloaded update method.
   *
   * Expects two Consumers that invoke setField and addToField respectively on the named field, passing in
   * a provided value.
   *
   * The Consumer / Lambda Expression approach is used here to avoid code duplication between the various
   * update methods. It is not possible to make update() a generic method because ultimately it would need to call
   * one of the specific setField or addToField methods which in turn call data.put(String, String),
   * data.put(String, Long), data.put(String Boolean)
   */
  private void update(String name, UpdateMode mode, Consumer setter, Consumer adder, Object... values) {

    if (values.length == 0)
      return;

    if (has(name) && mode.equals(UpdateMode.SKIP)) {
      return;
    }

    int i = 0;
    if (mode.equals(UpdateMode.OVERWRITE)) {
      setter.accept(values[0]);
      i = 1;
    }
    for (; i < values.length; i++) {
      adder.accept(values[i]);
    }
  }

  public void setField(String name, String value) {
    data.put(name, value);
  }

  public void setField(String name, Long value) {
    data.put(name, value);
  }

  public void setField(String name, Integer value) {
    data.put(name, value);
  }

  public void setField(String name, Boolean value) {
    data.put(name, value);
  }

  public void setField(String name, Double value) { data.put(name, value);}

  public void renameField(String oldName, String newName, UpdateMode mode) {
    JsonNode oldValues = data.get(oldName);
    data.remove(oldName);

    if (has(newName)) {
      if (mode.equals(UpdateMode.SKIP)) {
        return;
      } else if (mode.equals(UpdateMode.APPEND)) {
        convertToList(newName);

        if (oldValues.getNodeType() == JsonNodeType.ARRAY) {
          data.withArray(newName).addAll((ArrayNode) oldValues);
        } else {
          data.withArray(newName).add(oldValues);
        }
        return;
      }
    }

    data.set(newName,oldValues);
  }

  // This will return null in two cases : 1) If the field is absent 2) IF the field is present but contains a null.
  // To distinguish between these, you can call has().
  // Calling getString for a field which is multivalued will return the first value in the list of Strings.
  public String getString(String name) {
    if (!data.has(name)) {
      return null;
    }

    JsonNode node;
    if (isMultiValued(name)) {
      node = data.withArray(name).get(0);
    } else {
      node = data.get(name);
    }

    return node.isNull() ? null : node.asText();
  }

  public List<String> getStringList(String name) {
    if (!data.has(name)) {
      return null;
    }

    if (!isMultiValued(name)) {
      return Collections.singletonList(getString(name));
    }

    ArrayNode array = data.withArray(name);
    List<String> result = new ArrayList<>();
    for (JsonNode node : array) {
      result.add(node.isNull() ? null : node.asText());
    }
    return result;
  }

  public String getId() {
    return getString(ID_FIELD);
  }

  public String getRunId() {
    return getString(RUNID_FIELD);
  }

  public boolean has(String name) {
    return data.has(name);
  }

  public boolean hasNonNull(String name) {
    return data.hasNonNull(name);
  }

  public boolean isMultiValued(String name) {
     return data.has(name) && data.get(name).getNodeType() == JsonNodeType.ARRAY;
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

  public void addToField(String name, Long value) {
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

  public void addToField(String name, Double value) {
    convertToList(name);
    ArrayNode array = data.withArray(name);
    array.add(value);
  }

  public void logError(String description) {
    addToField(ERROR_FIELD, description);
  }

  public Map<String,Object> asMap() {
    Map<String, Object> result = MAPPER.convertValue(data, TYPE);
    return result;
  }

  public void addChild(Document document) {
    ArrayNode node = data.withArray(CHILDREN_FIELD);
    node.add(document.data);
  }

  public List<Document> getChildren() {
    ArrayNode node = data.withArray(CHILDREN_FIELD);
    ArrayList<Document> children = new ArrayList();
    for (Iterator<JsonNode> it = node.elements(); it.hasNext(); ) {
      JsonNode element = it.next();
      try {
        children.add(new Document(element.deepCopy()));
      } catch (DocumentException e) {
        log.error("Unable to instantiate child Document", e);
      }
    }
    return children;
  }

  @Override
  public String toString() {
    return data.toString();
  }

  @Override
  public Document clone() {
    try {
      return new Document(data.deepCopy());
    } catch (DocumentException e) {
      throw new IllegalStateException("Document not cloneable", e);
    }
  }
}
