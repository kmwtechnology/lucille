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

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

/**
 * A record from a source system to be passed through a Pipeline, enriched,
 * and sent to a destination system.
 *
 */
public class Document implements Cloneable {

  public static final String ID_FIELD = "id";
  public static final String RUNID_FIELD = "run_id";
  public static final String ERROR_FIELD = "errors";
  public static final String CHILDREN_FIELD = ".children";

  public static final List<String> RESERVED_FIELDS = List.of(ID_FIELD, RUNID_FIELD, CHILDREN_FIELD);

  protected static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TypeReference<Map<String, Object>> TYPE = new TypeReference<Map<String, Object>>(){};
  private static final Logger log = LoggerFactory.getLogger(Document.class);

  protected final ObjectNode data;

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

  public static Document fromJsonString(String json, UnaryOperator<String> idUpdater) throws DocumentException, JsonProcessingException {
    Document doc = fromJsonString(json);
    doc.data.put(ID_FIELD, idUpdater.apply(doc.getId()));
    return doc;
  }

  public void removeField(String name) {
    validateNotReservedField(name);
    data.remove(name);
  }

  public void removeFromArray(String name, int index) {
    validateNotReservedField(name);
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
    update(name, mode, (v)->{setField(name,(String)v);}, (v)->{setOrAdd(name,(String)v);}, values);
  }

  public void update(String name, UpdateMode mode, Long... values) {
    update(name, mode, (v)->{setField(name,(Long)v);}, (v)->{setOrAdd(name,(Long)v);}, values);
  }

  public void update(String name, UpdateMode mode, Integer... values) {
    update(name, mode, (v)->{setField(name,(Integer)v);}, (v)->{setOrAdd(name,(Integer)v);}, values);
  }

  public void update(String name, UpdateMode mode, Boolean... values) {
    update(name, mode, (v)->{setField(name,(Boolean)v);}, (v)->{setOrAdd(name,(Boolean)v);}, values);
  }

  public void update(String name, UpdateMode mode, Double... values) {
    update(name, mode, (v)->{setField(name,(Double)v);}, (v)->{setOrAdd(name,(Double)v);}, values);
  }

  public void update(String name, UpdateMode mode, Instant... values) {
    update(name, mode, (v)->{setField(name,(Instant)v);}, (v)->{setOrAdd(name,(Instant)v);}, values);
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

    validateNotReservedField(name);

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

  public void initializeRunId(String value) {
    if (data.has(RUNID_FIELD)) {
      throw new IllegalStateException();
    }
    data.put(RUNID_FIELD, value);
  }

  public void clearRunId() {
    if (data.has(RUNID_FIELD)) {
      data.remove(RUNID_FIELD);
    }
  }

  public void setField(String name, String value) {
    validateNotReservedField(name);
    data.put(name, value);
  }

  public void setField(String name, Long value) {
    validateNotReservedField(name);
    data.put(name, value);
  }

  public void setField(String name, Integer value) {
    validateNotReservedField(name);
    data.put(name, value);
  }

  public void setField(String name, Boolean value) {
    validateNotReservedField(name);
    data.put(name, value);
  }

  public void setField(String name, Double value) {
    validateNotReservedField(name);
    data.put(name, value);
  }

  public void setField(String name, JsonNode value) {
    validateNotReservedField(name);
    data.set(name, value);
  }

  public void setField(String name, Instant value) {
    validateNotReservedField(name);
    String instantStr = DateTimeFormatter.ISO_INSTANT.format(value);
    data.put(name, instantStr);
  }

  public void renameField(String oldName, String newName, UpdateMode mode) {
    validateNotReservedField(oldName);
    validateNotReservedField(newName);
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

    JsonNode node = getSingleNode(name);

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

  public Integer getInt(String name) {
    if (!data.has(name)) {
      return null;
    }

    JsonNode node = getSingleNode(name);

    return node.isNull() ? null : node.asInt();
  }

  public List<Integer> getIntList(String name) {
    if (!data.has(name)) {
      return null;
    }

    if (!isMultiValued(name)) {
      return Collections.singletonList(getInt(name));
    }

    ArrayNode array = data.withArray(name);
    List<Integer> result = new ArrayList<>();
    for (JsonNode node : array) {
      result.add(node.isNull() ? null : node.asInt());
    }
    return result;
  }

  public Double getDouble(String name) {
    if (!data.has(name)) {
      return null;
    }

    JsonNode node = getSingleNode(name);

    return node.isNull() ? null : node.asDouble();
  }

  public List<Double> getDoubleList(String name) {
    if (!data.has(name)) {
      return null;
    }

    if (!isMultiValued(name)) {
      return Collections.singletonList(getDouble(name));
    }

    ArrayNode array = data.withArray(name);
    List<Double> result = new ArrayList<>();
    for (JsonNode node : array) {
      result.add(node.isNull() ? null : node.asDouble());
    }
    return result;
  }

  public Boolean getBoolean(String name) {
    if (!data.has(name)) {
      return null;
    }

    JsonNode node = getSingleNode(name);

    return node.isNull() ? null : node.asBoolean();
  }

  public List<Boolean> getBooleanList(String name) {
    if (!data.has(name)) {
      return null;
    }

    if (!isMultiValued(name)) {
      return Collections.singletonList(getBoolean(name));
    }

    ArrayNode array = data.withArray(name);
    List<Boolean> result = new ArrayList<>();
    for (JsonNode node : array) {
      result.add(node.isNull() ? null : node.asBoolean());
    }
    return result;
  }

  public Long getLong(String name) {
    if (!data.has(name)) {
      return null;
    }

    JsonNode node = getSingleNode(name);

    return node.isNull() ? null : node.asLong();
  }

  public List<Long> getLongList(String name) {
    if (!data.has(name)) {
      return null;
    }

    if (!isMultiValued(name)) {
      return Collections.singletonList(getLong(name));
    }

    ArrayNode array = data.withArray(name);
    List<Long> result = new ArrayList<>();
    for (JsonNode node : array) {
      result.add(node.isNull() ? null : node.asLong());
    }
    return result;
  }

  public Instant getInstant(String name) {
    if (!data.has(name)) {
      return null;
    }

    JsonNode node = getSingleNode(name);

    String dateStr = node.asText();
    Instant dateInstant = Instant.from(DateTimeFormatter.ISO_INSTANT.parse(dateStr));
    return node.isNull() ? null : dateInstant;
  }

  public List<Instant> getInstantList(String name) {
    if (!data.has(name)) {
      return null;
    }

    if (!isMultiValued(name)) {
      return Collections.singletonList(getInstant(name));
    }

    ArrayNode array = data.withArray(name);
    List<Instant> result = new ArrayList<>();
    for (JsonNode node : array) {
      String instantStr = node.asText();
      Instant instant = Instant.from(DateTimeFormatter.ISO_INSTANT.parse(instantStr));
      result.add(node.isNull() ? null : instant);
    }
    return result;
  }

  private JsonNode getSingleNode(String name) {
    return isMultiValued(name) ? data.withArray(name).get(0) : data.get(name);
  }

  public int length(String name) {
    if (!has(name)) {
      return 0;
    } else if (!isMultiValued(name)) {
      return 1;
    } else {
      return data.get(name).size();
    }
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
    return data.has(name) && JsonNodeType.ARRAY.equals(data.get(name).getNodeType());
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other instanceof Document) {
      return data.equals(((Document)other).data);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return data.hashCode();
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
    validateNotReservedField(name);
    convertToList(name);
    ArrayNode array = data.withArray(name);
    array.add(value);
  }

  public void addToField(String name, Long value) {
    validateNotReservedField(name);
    convertToList(name);
    ArrayNode array = data.withArray(name);
    array.add(value);
  }

  public void addToField(String name, Integer value) {
    validateNotReservedField(name);
    convertToList(name);
    ArrayNode array = data.withArray(name);
    array.add(value);
  }

  public void addToField(String name, Boolean value) {
    validateNotReservedField(name);
    convertToList(name);
    ArrayNode array = data.withArray(name);
    array.add(value);
  }

  public void addToField(String name, Double value) {
    validateNotReservedField(name);
    convertToList(name);
    ArrayNode array = data.withArray(name);
    array.add(value);
  }

  /**
   * Converts a given date in Instant form to a string according to DateTimeFormatter.ISO_INSTANT,
   * it can then be accessed as a string via getString() or a converted back to an Instant via getInstant().
   * @param name
   * @param value
   */
  public void addToField(String name, Instant value) {
    validateNotReservedField(name);
    convertToList(name);
    ArrayNode array = data.withArray(name);
    String dateStr = DateTimeFormatter.ISO_INSTANT.format(value);
    array.add(dateStr);
  }


  /**
   * Sets the field to the given value if the field is not already present; otherwise adds it to the field.
   *
   * If the field does not already exist and this method is called once, the field will be created as single-valued;
   * if the field already exists and/or this method is called more than once, the field will converted to a list
   * of values.
   *
   */
  public void setOrAdd(String name, String value) {
    if (has(name)) {
      addToField(name, value);
    } else {
      setField(name, value);
    }
  }

  public void setOrAdd(String name, Long value) {
    if (has(name)) {
      addToField(name, value);
    } else {
      setField(name, value);
    }
  }

  public void setOrAdd(String name, Integer value) {
    if (has(name)) {
      addToField(name, value);
    } else {
      setField(name, value);
    }
  }

  public void setOrAdd(String name, Boolean value) {
    if (has(name)) {
      addToField(name, value);
    } else {
      setField(name, value);
    }
  }

  public void setOrAdd(String name, Double value) {
    if (has(name)) {
      addToField(name, value);
    } else {
      setField(name, value);
    }
  }

  /**
   * Adds a given date in Instant form to a document according to DateTimeFormatter.ISO_INSTANT,
   * can then be accessed as a string via getString() or a converted back to an Instant via getInstant().
   * @param name
   * @param value
   */
  public void setOrAdd(String name, Instant value) {
    if (has(name)) {
      addToField(name, value);
    } else {
      setField(name, value);
    }
  }

  /**
   * Adds a given field from the designated "other" document to the current document.
   * If a field is already present on the current document, the field is converted to a list
   * IllegalArgumentException is thrown if this method is called with a reserved field like id.
   *
   */
  public void setOrAdd(String name, Document other) throws IllegalArgumentException {
    validateNotReservedField(name);

    if (!has(name)) {

      if (!other.has(name)) {
        return;
      } else {
        data.set(name, other.data.get(name));
        return;
      }

    } else {

      convertToList(name);
      ArrayNode currentValues = (ArrayNode) data.get(name);
      JsonNode otherValue = other.data.get(name);

      if (otherValue.getNodeType() == JsonNodeType.ARRAY) {
        currentValues.addAll((ArrayNode) otherValue);
      } else {
        currentValues.add(otherValue);
      }

    }
  }

  /**
   * Adds all the fields of the designated "other" document to the current document, excluding reserved fields
   * like id. If a field is already present on the current document, the field is converted to a list
   * and the new value is appended.
   *
   */
  public void setOrAddAll(Document other) {
    for (Iterator<String> it = other.data.fieldNames(); it.hasNext(); ) {
      String name = it.next();
      if (RESERVED_FIELDS.contains(name)) {
        continue;
      }
      setOrAdd(name, other);
    }
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

  public boolean hasChildren() {
    if (!data.has(CHILDREN_FIELD)) {
      return false;
    }
    if (getChildren().isEmpty()) {
      return false;
    }
    return true;
  }

  public List<Document> getChildren() {
    if (!data.has(CHILDREN_FIELD)) {
      return new ArrayList();
    }
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

  public Document cloneWithNewId(String newId) {
    Document doc = clone();
    doc.data.put(Document.ID_FIELD, newId);
    return doc;
  }

  private void validateNotReservedField(String name) throws IllegalArgumentException {
    if (RESERVED_FIELDS.contains(name)) {
      throw new IllegalArgumentException();
    }
  }

  public Set<String> getFieldNames() {
    Set<String> fieldNames = new HashSet<String>();
    Iterator<String> it = data.fieldNames();
    while (it.hasNext()) {
      String fieldName = it.next();
      fieldNames.add(fieldName);
    }
    return fieldNames;
  }

  /**
   * A method to remove duplicate values from multivalued fields in a document and place the values into a target field.
   * If the target field is null or the same as the original field, then modification will happen in place.
   *
   * @param fieldName the field to remove duplicate values from
   * @param targetFieldName the field to copy to
   */
  public void removeDuplicateValues(String fieldName, String targetFieldName) {
    if (!isMultiValued(fieldName)) {
      return;
    }

    ArrayNode arrayNode = data.withArray(fieldName);
    LinkedHashSet<JsonNode> set = new LinkedHashSet<>();
    int length = 0;
    for (JsonNode jsonNode : arrayNode) {
      length++;
      set.add(jsonNode);
    }

    if (targetFieldName == null || fieldName.equals(targetFieldName)) {
      if (set.size() == length) {
        return;
      }
      data.remove(fieldName);
      arrayNode = data.withArray(fieldName);
      for (JsonNode jsonNode : set) {
        arrayNode.add(jsonNode);
      }
    } else {
      arrayNode = data.withArray(targetFieldName);
      for (JsonNode jsonNode : set) {
        arrayNode.add(jsonNode);
      }
    }
  }
}
