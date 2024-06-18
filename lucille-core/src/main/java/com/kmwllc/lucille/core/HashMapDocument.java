package com.kmwllc.lucille.core;

import com.api.jsonata4java.expressions.Expressions;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.util.LinkedMultiMap;
import com.kmwllc.lucille.util.MultiMap;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import javax.naming.OperationNotSupportedException;


@JsonIgnoreProperties(value = {"fieldNames", "runId", "dropped", "id", "children"})
public class HashMapDocument implements Document, Serializable {

  static final long serialVersionUID = 1L;

  protected static final ObjectMapper MAPPER = new ObjectMapper();

  private static final Function<Object, Integer> TO_INT =
      value -> {
        if (value instanceof String) {
          return Integer.parseInt((String) value);
        } else {
          return ((Number) value).intValue();
        }
      };

  private LinkedMultiMap data;

  public void setData(LinkedMultiMap data) {
    this.data = data;
  }

  public LinkedMultiMap getData() {
    return data;
  }

  public HashMapDocument() {
  }

  public HashMapDocument(String id) {
    if (id == null || id.isEmpty()) {
      throw new NullPointerException("ID cannot be null or empty");
    }
    data = new LinkedMultiMap();
    data.putOne(ID_FIELD, id);
  }

  public HashMapDocument(String id, String runId) {
    this(id);
    if (runId == null || runId.isEmpty()) {
      throw new IllegalArgumentException("runId cannot be null or empty");
    }
    data.putOne(RUNID_FIELD, runId);
  }

  public HashMapDocument(Document other) {
    this(getData(other).deepCopy());
  }

  public HashMapDocument(MultiMap other) {
    data = (LinkedMultiMap) other;
  }

  public HashMapDocument(ObjectNode data) throws DocumentException {
    this(data, null);
  }

  public HashMapDocument(ObjectNode node, UnaryOperator<String> idUpdater)
      throws DocumentException {
    if (node.getNodeType() != JsonNodeType.OBJECT) {
      throw new DocumentException("data is not an object");
    }

    if (!node.hasNonNull(ID_FIELD)) {
      throw new DocumentException("id is missing");
    }

    data = new LinkedMultiMap();
    data.putOne(ID_FIELD, updateString(requireString(node.get(ID_FIELD)), idUpdater));

    for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
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
          for (JsonNode child : value) {
            addChild(new HashMapDocument((ObjectNode) child, idUpdater));
          }
          break;
        default:
          addNodeValueToField(key, value);
      }
    }
  }

  // todo consider what happens for nested arrays
  public void addNodeValueToField(String name, JsonNode node) {

    validateFieldNames(name);

    switch (node.getNodeType()) {
      case STRING:
        setOrAddGeneric(name, node.asText());
        break;
      case NUMBER:
        if (node.isInt()) {
          setOrAddGeneric(name, node.asInt());
        } else if (node.isDouble()) {
          setOrAddGeneric(name, node.asDouble());
        } else if (node.isLong()) {
          setOrAddGeneric(name, node.asLong());
        } else {
          throw new IllegalArgumentException("Unsupported number type: " + node);
        }
        break;
      case NULL:
        // fall through
        setOrAddGeneric(name, null);
        break;
      case OBJECT:
        setOrAddGeneric(name, node);
        break;
      //        throw new UnsupportedOperationException(name + " field is an object");
      //        addGeneric(name, node); // todo does this have to be parsed?
      //        break;
      case ARRAY:
        data.putMany(name, new ArrayList<>()); // initialize because may be empty
        for (JsonNode item : node) {
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
    return new HashMapDocument((ObjectNode) MAPPER.readTree(json), idUpdater);
  }

  @Override
  public void removeField(String name) {
    validateFieldNames(name);
    data.remove(name);
  }

  @Override
  public void removeFromArray(String name, int index) {
    validateFieldNames(name);
    data.removeFromArray(name, index);
  }


  @Override
  public void initializeRunId(String value) {
    if (value == null || value.isEmpty()) {
      throw new IllegalArgumentException("RunId cannot be null or empty");
    }
    if (has(RUNID_FIELD)) {
      throw new IllegalStateException("RunId already set");
    }
    data.putOne(RUNID_FIELD, value);
  }

  @Override
  public void clearRunId() {
    if (has(RUNID_FIELD)) {
      data.remove(RUNID_FIELD);
    }
  }

  private <T> void setFieldGeneric(String name, T value) {
    validateFieldNames(name);
    data.putOne(name, value);
  }

  @Override
  public void setField(String name, String value) {
    setFieldGeneric(name, value);
  }

  @Override
  public void setField(String name, Long value) {
    setFieldGeneric(name, value);
  }

  @Override
  public void setField(String name, Integer value) {
    setFieldGeneric(name, value);
  }

  @Override
  public void setField(String name, Boolean value) {
    setFieldGeneric(name, value);
  }

  @Override
  public void setField(String name, Double value) {
    setFieldGeneric(name, value);
  }

  @Override
  public void setField(String name, Float value) {
    setFieldGeneric(name, value);
  }

  @Override
  public void setField(String name, JsonNode value) {
    setFieldGeneric(name, value);
  }

  @Override
  public void setField(String name, Instant value) {
    setFieldGeneric(name, value);
  }

  @Override
  public void setField(String name, byte[] value) {
    setFieldGeneric(name, value);
  }

  @Override
  public void renameField(String oldName, String newName, UpdateMode mode) {
    validateFieldNames(oldName, newName);
    if (oldName != null && oldName.equals(newName)) {
      return;
    }

    if (has(newName)) {
      switch (mode) {
        case OVERWRITE:
          removeField(newName);
          // go to next step
          break;
        case APPEND:
          data.addAll(newName, data.getMany(oldName));
          data.remove(oldName);
          return;
        case SKIP:
          data.remove(oldName);
          return;
        default:
          throw new UnsupportedOperationException("Unsupported mode " + mode);
      }
    }

    data.rename(oldName, newName);
  }

  private <T> T convertOrNull(Object value, Function<Object, T> converter) {
    // TODO !!! value types should be changed during add
    // todo add a try catch with an informative error message
    return value == null ? null : converter.apply(value);
  }

  private <T> T getValue(String name, Function<Object, T> converter) {
    return !has(name) ? null : convertOrNull(data.getOne(name), converter);
  }

  private <T> List<T> getValues(String name, Function<Object, T> converter) {
    return !has(name)
        ? null
        : (List<T>) data.getMany(name).stream()
            .map(value -> convertOrNull(value, converter))
            .collect(Collectors.toList());
  }

  @Override
  public String getString(String name) {
    return getValue(name, String::valueOf);
  }

  @Override
  public List<String> getStringList(String name) {
    return getValues(name, String::valueOf);
  }

  @Override
  public Integer getInt(String name) {
    return getValue(name, TO_INT);
  }

  @Override
  public List<Integer> getIntList(String name) {
    return getValues(name, TO_INT);
  }

  @Override
  public Double getDouble(String name) {
    return getValue(name, value -> ((Number) value).doubleValue());
  }

  @Override
  public List<Double> getDoubleList(String name) {
    return getValues(name, value -> ((Number) value).doubleValue());
  }

  @Override
  public Float getFloat(String name) {
    return getValue(name, value -> ((Number) value).floatValue());
  }

  @Override
  public List<Float> getFloatList(String name) {
    return getValues(name, value -> ((Number) value).floatValue());
  }

  @Override
  public Boolean getBoolean(String name) {
    return getValue(name, value -> (Boolean) value);
  }

  @Override
  public List<Boolean> getBooleanList(String name) {
    return getValues(name, value -> (Boolean) value);
  }

  @Override
  public Long getLong(String name) {
    return getValue(name, value -> ((Number) value).longValue());
  }

  @Override
  public List<Long> getLongList(String name) {
    return getValues(name, value -> ((Number) value).longValue());
  }

  @Override
  public Instant getInstant(String name) {
    return getValue(name, value -> (Instant) value);
  }

  @Override
  public byte[] getBytes(String name) {
    return getValue(name, value -> (byte[]) value);
  }

  @Override
  public JsonNode getJson(String name) {
    return getValue(name, value -> (JsonNode) value);
  }

  @Override
  public List<Instant> getInstantList(String name) {
    return getValues(name, value -> (Instant) value);
  }

  @Override
  public List<byte[]> getBytesList(String name) {
    return getValues(name, value -> (byte[]) value);
  }

  @Override
  public List<JsonNode> getJsonList(String name) {
    return getValues(name, value -> (JsonNode) value);
  }

  @Override
  public int length(String name) {
    return has(name) ? data.length(name) : 0;
  }

  @Override
  public String getId() {
    return (String) data.getOne(ID_FIELD);
  }

  @Override
  public String getRunId() {
    if (has(RUNID_FIELD)) {
      return (String) data.getOne(RUNID_FIELD);
    }
    return null;
  }

  @Override
  public boolean has(String name) {
    return data.contains(name);
  }

  @Override
  public boolean hasNonNull(String name) {
    // todo check if need to check multiple items for non null
    return data.contains(name) && data.getOne(name) != null;
  }

  @Override
  public boolean isMultiValued(String name) {
    return has(name) && data.isMultiValued(name);
  }

  private <T> void addToFieldGeneric(String name, T value) {
    validateFieldNames(name);
    data.add(name, value);
  }

  @Override
  public void addToField(String name, String value) {
    addToFieldGeneric(name, value);
  }

  @Override
  public void addToField(String name, Long value) {
    addToFieldGeneric(name, value);
  }

  @Override
  public void addToField(String name, Integer value) {
    addToFieldGeneric(name, value);
  }

  @Override
  public void addToField(String name, Boolean value) {
    addToFieldGeneric(name, value);
  }

  @Override
  public void addToField(String name, Double value) {
    addToFieldGeneric(name, value);
  }

  @Override
  public void addToField(String name, Float value) {
    addToFieldGeneric(name, value);
  }

  @Override
  public void addToField(String name, Instant value) {
    addToFieldGeneric(name, value);
  }

  @Override
  public void addToField(String name, byte[] value) {
    addToFieldGeneric(name, value);
  }

  @Override
  public void addToField(String name, JsonNode value) {
    addToFieldGeneric(name, value);
  }

  private <T> void setOrAddGeneric(String name, T value) {
    validateFieldNames(name);
    data.setOrAdd(name, value);
  }

  @Override
  public void setOrAdd(String name, String value) {
    setOrAddGeneric(name, value);
  }

  @Override
  public void setOrAdd(String name, Long value) {
    setOrAddGeneric(name, value);
  }

  @Override
  public void setOrAdd(String name, Integer value) {
    setOrAddGeneric(name, value);
  }

  @Override
  public void setOrAdd(String name, Boolean value) {
    setOrAddGeneric(name, value);
  }

  @Override
  public void setOrAdd(String name, Double value) {
    setOrAddGeneric(name, value);
  }

  @Override
  public void setOrAdd(String name, Float value) {
    setOrAddGeneric(name, value);
  }

  @Override
  public void setOrAdd(String name, Instant value) {
    setOrAddGeneric(name, value);
  }

  @Override
  public void setOrAdd(String name, byte[] value) {
    setOrAddGeneric(name, value);
  }

  @Override
  public void setOrAdd(String name, JsonNode value) {
    setOrAddGeneric(name, value);
  }

  @Override
  public void setOrAdd(String name, Document other) throws IllegalArgumentException {
    validateFieldNames(name);

    if (!other.has(name)) {
      return;
      // the particular implementation does not require an error here
      // throw new IllegalArgumentException("The other document does not have the field " + name);
    }

    MultiMap otherData = getData(other);
    if (has(name) || other.isMultiValued(name)) {
      data.addAll(name, otherData.getMany(name));
    } else {
      data.putOne(name, otherData.getOne(name));
    }
  }

  @Override
  public void setOrAddAll(Document other) {
    MultiMap otherData = getData(other);
    for (String name : otherData.getKeys()) {
      if (RESERVED_FIELDS.contains(name)) {
        continue;
      }
      setOrAdd(name, other);
    }
  }

  @Override
  public Map<String, Object> asMap() {
    // create a shallow copy of the document data
    // adding, replacing, or removing entries in the returned map will not affect the original document
    // however, updates made to an object inside the map (for example, changing the contents of an array value) will
    // be evident when that same object is retrieved from the original document
    return (Map<String, Object>)data.getData().clone();
  }

  @Override
  public void addChild(Document document) {
    if (document == null) {
      throw new IllegalArgumentException("The document is null");
    }
    data.add(CHILDREN_FIELD, document.asMap());
  }

  @Override
  public boolean hasChildren() {
    return data.contains(CHILDREN_FIELD) && data.getMany(CHILDREN_FIELD).size() > 0;
  }

  public void removeChildren() {
    data.remove(CHILDREN_FIELD);
  }
  
  @Override
  public List<Document> getChildren() {
    if (!hasChildren()) {
      return Collections.emptyList();
    }
    return (List<Document>) data.getMany(CHILDREN_FIELD).stream()
        .map(child -> new HashMapDocument(new LinkedMultiMap((LinkedHashMap) child)))
        .collect(Collectors.toList());
  }

  @Override
  public Set<String> getFieldNames() {
    return data.getKeys();
  }

  @Override
  public boolean isDropped() {
    return data.contains(DROP_FIELD);
  }

  @Override
  public void setDropped(boolean status) {
    if (status) {
      data.putOne(DROP_FIELD, true);
    } else {
      data.remove(DROP_FIELD);
    }
  }

  @Override
  public void removeDuplicateValues(String source, String target) {
    validateFieldNames(source);

    if (target != null && !target.equals(source)) {
      data.putMany(target, new ArrayList<>(new LinkedHashSet<>(data.getMany(source))));
    } else if (isMultiValued(source)) {
      data.removeDuplicates(source);
    }
  }

  @Override
  public Document deepCopy() {
    return new HashMapDocument(this);
  }

  @Override
  public void transform(Expressions expr) throws DocumentException {
    throw new UnsupportedOperationException("Transform is not supported for HashMap implementation of Document");
  }

  @Override
  public String toString() {
    try {
      return MAPPER.writeValueAsString(asMap());
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return data.equals(((HashMapDocument) o).data);
  }

  @Override
  public int hashCode() {
    return data.hashCode();
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

  private static MultiMap getData(Document other) {
    if (other == null) {
      throw new IllegalStateException("Document is null");
    }
    if (!(other instanceof HashMapDocument)) {
      throw new IllegalStateException("Documents are not of the same type");
    }
    return ((HashMapDocument) other).data;
  }
}
