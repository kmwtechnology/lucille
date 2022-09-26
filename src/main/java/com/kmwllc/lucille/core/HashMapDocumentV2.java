package com.kmwllc.lucille.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.util.IMultiMap;
import com.kmwllc.lucille.util.MultiMap;

import java.time.Instant;
import java.util.*;
import java.util.function.UnaryOperator;

import static com.kmwllc.lucille.core.HashMapDocument.SUPPORTED_TYPES;

public class HashMapDocumentV2 extends AbstractDocument {

  private static final ObjectMapper MAPPER = new ObjectMapper();


  private final IMultiMap data;

  //  public HashMapDocumentV2(Document document) {
//    // todo
//  }

  public HashMapDocumentV2(String id) {
    if (id == null) {
      throw new NullPointerException("ID cannot be null");
    }

    data = new MultiMap(SUPPORTED_TYPES);
    data.putOne(ID_FIELD, id);
  }

  public HashMapDocumentV2(String id, String runId) {
    this(id);

    if (runId == null) {
      throw new IllegalArgumentException("runId cannot be null");
    }
    data.putOne(RUNID_FIELD, runId);
  }

  public HashMapDocumentV2(ObjectNode data) throws DocumentException {
    this(data, null);
  }

  public HashMapDocumentV2(ObjectNode node, UnaryOperator<String> idUpdater) throws DocumentException {
    if (node.getNodeType() != JsonNodeType.OBJECT) {
      throw new DocumentException("data is not an object");
    }

    if (!node.hasNonNull(ID_FIELD)) {
      throw new DocumentException("id is missing");
    }

    this.data = new MultiMap(SUPPORTED_TYPES);
    data.putOne(ID_FIELD, updateString(requireString(node.get(ID_FIELD)), idUpdater));

    for(Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext();) {
      Map.Entry<String, JsonNode> entry = it.next();
      String key = entry.getKey();
      JsonNode value = entry.getValue();

      switch (key) {
        case ID_FIELD:
          break;
        case RUNID_FIELD:
          data.putOne(RUNID_FIELD, requireString(value));
          break;
        case CHILDREN_FIELD:
          if (value.getNodeType() != JsonNodeType.ARRAY) {
            throw new IllegalArgumentException();
          }
          for (JsonNode child: value) {
            addChild(new HashMapDocumentV2((ObjectNode) child, idUpdater));
          }
          break;
        default:
          addNodeValueToField(key, value);
      }
    }
  }

  // todo consider what happens for nested arrays
  // todo need long here and other types?
  public void addNodeValueToField(String name, JsonNode node) {
    switch (node.getNodeType()) {
      case STRING:
        addGeneric(name, node.asText());
        break;
      case NUMBER:
        if (node.isInt()) {
          addGeneric(name, node.asInt());
        } else if (node.isDouble()) {
          addGeneric(name, node.asDouble());
        } else if (node.isLong()) {
          addGeneric(name, node.asLong());
        } else {
          throw new IllegalArgumentException("Unsupported number type: " + node);
        }
        break;
      case NULL:
        // fall through
      case OBJECT:
        addGeneric(name, node);
        break;
      case ARRAY:
        data.putMany(name, new ArrayList<>()); // initialize because may be empty
        for (JsonNode item: node) {
          addNodeValueToField(name, item);
        }
        break;
      default:
        throw new UnsupportedOperationException("Unsupported type " + node.getNodeType());
    }
  }

  public static Document fromJsonString(String json)
    throws DocumentException, JsonProcessingException {
    return fromJsonString(json, null);
  }

  public static Document fromJsonString(String json, UnaryOperator<String> idUpdater)
    throws DocumentException, JsonProcessingException {
    return new HashMapDocumentV2((ObjectNode) MAPPER.readTree(json), idUpdater);
  }

  private <T> void addGeneric(String name, T value) {
    validateNotReservedField(name);

    // todo preprocess value to match the field type

    data.add(name, value);

//    // todo review this
//    if (types.containsKey(name) && types.get(name) != value.getClass()) {
//
//      if (value.getClass() == Integer.class && types.get(name) == Double.class) {
//        data.get(name).add(((Integer) value).doubleValue());
//      }
//      else {
//        throw new UnsupportedOperationException();
//      }
//
//    } else {
//      data.get(name).add(value);
//    }
  }

  private void validateNotReservedField(String... names) throws IllegalArgumentException {

    if (names == null) {
      throw new IllegalArgumentException("expecting string parameters");
    }

    for (String name: names) {
      if (name == null) {
        throw new IllegalArgumentException("Field name cannot be null");
      }

      if (RESERVED_FIELDS.contains(name)) {
        throw new IllegalArgumentException(name + " is a reserved field");
      }
    }
  }



  @Override
  public void removeField(String name) {

  }

  @Override
  public void removeFromArray(String name, int index) {

  }

  @Override
  public void update(String name, UpdateMode mode, String... values) {

  }

  @Override
  public void update(String name, UpdateMode mode, Long... values) {

  }

  @Override
  public void update(String name, UpdateMode mode, Integer... values) {

  }

  @Override
  public void update(String name, UpdateMode mode, Boolean... values) {

  }

  @Override
  public void update(String name, UpdateMode mode, Double... values) {

  }

  @Override
  public void update(String name, UpdateMode mode, Instant... values) {

  }

  @Override
  public void initializeRunId(String value) {

  }

  @Override
  public void clearRunId() {

  }

  @Override
  public void setField(String name, String value) {

  }

  @Override
  public void setField(String name, Long value) {

  }

  @Override
  public void setField(String name, Integer value) {

  }

  @Override
  public void setField(String name, Boolean value) {

  }

  @Override
  public void setField(String name, Double value) {

  }

  @Override
  public void setField(String name, JsonNode value) {

  }

  @Override
  public void setField(String name, Instant value) {

  }

  @Override
  public void renameField(String oldName, String newName, UpdateMode mode) {

  }

  @Override
  public ObjectNode getData() {
    return null;
  }

  @Override
  public String getString(String name) {
    return null;
  }

  @Override
  public List<String> getStringList(String name) {
    return null;
  }

  @Override
  public Integer getInt(String name) {
    return null;
  }

  @Override
  public List<Integer> getIntList(String name) {
    return null;
  }

  @Override
  public Double getDouble(String name) {
    return null;
  }

  @Override
  public List<Double> getDoubleList(String name) {
    return null;
  }

  @Override
  public Boolean getBoolean(String name) {
    return null;
  }

  @Override
  public List<Boolean> getBooleanList(String name) {
    return null;
  }

  @Override
  public Long getLong(String name) {
    return null;
  }

  @Override
  public List<Long> getLongList(String name) {
    return null;
  }

  @Override
  public Instant getInstant(String name) {
    return null;
  }

  @Override
  public List<Instant> getInstantList(String name) {
    return null;
  }

  @Override
  public int length(String name) {
    return 0;
  }

  @Override
  public String getId() {
    return null;
  }

  @Override
  public String getRunId() {
    return null;
  }

  @Override
  public boolean has(String name) {
    return false;
  }

  @Override
  public boolean hasNonNull(String name) {
    return false;
  }

  @Override
  public boolean isMultiValued(String name) {
    return false;
  }

  @Override
  public void addToField(String name, String value) {

  }

  @Override
  public void addToField(String name, Long value) {

  }

  @Override
  public void addToField(String name, Integer value) {

  }

  @Override
  public void addToField(String name, Boolean value) {

  }

  @Override
  public void addToField(String name, Double value) {

  }

  @Override
  public void addToField(String name, Instant value) {

  }

  @Override
  public void setOrAdd(String name, String value) {

  }

  @Override
  public void setOrAdd(String name, Long value) {

  }

  @Override
  public void setOrAdd(String name, Integer value) {

  }

  @Override
  public void setOrAdd(String name, Boolean value) {

  }

  @Override
  public void setOrAdd(String name, Double value) {

  }

  @Override
  public void setOrAdd(String name, Instant value) {

  }

  @Override
  public void setOrAdd(String name, Document other) throws IllegalArgumentException {

  }

  @Override
  public void setOrAddAll(Document other) {

  }

  @Override
  public Map<String, Object> asMap() {
    return null;
  }

  @Override
  public void addChild(Document document) {

  }

  @Override
  public boolean hasChildren() {
    return false;
  }

  @Override
  public List<Document> getChildren() {
    return null;
  }

  @Override
  public Set<String> getFieldNames() {
    return null;
  }

  @Override
  public void removeDuplicateValues(String fieldName, String targetFieldName) {

  }

  @Override
  public Document deepCopy() {
    return null;
  }

  @Override
  public String toString() {
    return data.toString();
  }

  @Override
  public boolean equals(Object obj) {
    return false;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  private static String requireString(JsonNode node) throws DocumentException {
    if (!node.isTextual() || node.asText().isEmpty()) {
      throw new DocumentException("Expected non-empty string, got " + node);
    }
    return node.asText();
  }

  private static String updateString(String toUpdate, UnaryOperator<String> updater) {
    return updater == null ? toUpdate : updater.apply(toUpdate);
  }
}
