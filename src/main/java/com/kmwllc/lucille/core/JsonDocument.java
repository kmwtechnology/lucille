package com.kmwllc.lucille.core;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * A record from a source system to be passed through a Pipeline, enriched,
 * and sent to a destination system.
 *
 */
public class JsonDocument implements Document {

  private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ISO_INSTANT;
  private static final Function<String, Instant> DATE_PARSER =
    str -> Instant.from(DATE_TIME_FORMATTER.parse(str));
  private static final Function<Instant, String> INSTANT_FORMATTER = DATE_TIME_FORMATTER::format;

  protected static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TypeReference<Map<String, Object>> TYPE = new TypeReference<>() {};
  private static final Logger log = LoggerFactory.getLogger(Document.class);

  @JsonValue
  protected final ObjectNode data;

  /**
   * A copy constructor for {@link Document} that deep copies the ObjectNode and verifies the
   * validity of the document.
   *
   * @param document document to copy
   * @throws DocumentException if document is missing a nonempty {@link Document#ID_FIELD}
   */
  public JsonDocument(Document document) throws DocumentException {
    this(getData(document).deepCopy());
  }

  public JsonDocument(String id) {
    if (id == null || id.isEmpty()) {
      throw new NullPointerException("ID cannot be null or empty");
    }
    this.data = MAPPER.createObjectNode();
    this.data.put(ID_FIELD, id);
  }

  public JsonDocument(String id, String runId) {
    this(id);
    if (runId == null || runId.isEmpty()) {
      throw new IllegalStateException("runId cannot be null or empty");
    }
    this.data.put(RUNID_FIELD, runId);
  }

  /**
   * Creates a new {@link JsonDocument} from a {@link ObjectNode} of key/value pairs. Note: does
   * not defensively copy the given ObjectNode, so it must not be modified after creation.
   *
   * @param data the data to be stored in the document
   * @throws DocumentException if document is missing a nonempty {@link Document#ID_FIELD}
   */
  public JsonDocument(ObjectNode data) throws DocumentException {

    if (!data.hasNonNull(ID_FIELD)) {
      throw new DocumentException("id is missing");
    }

    JsonNode id = data.get(ID_FIELD);
    if (!id.isTextual() || id.asText().isEmpty()) {
      throw new DocumentException("id is present but null or empty or not a string");
    }

    this.data = data;
  }

  // todo where is the non null annotation coming from?
  public static JsonDocument fromJsonString(String json) throws DocumentException, JsonProcessingException {
    return fromJsonString(json, null);
  }

  public static JsonDocument fromJsonString(String json, UnaryOperator<String> idUpdater) throws DocumentException, JsonProcessingException {
    JsonDocument doc = new JsonDocument((ObjectNode) MAPPER.readTree(json));
    doc.data.put(ID_FIELD, idUpdater == null ? doc.getId() : idUpdater.apply(doc.getId()));
    return doc;
  }

  @Override
  public void removeField(String name) {
    validateNotReservedField(name);
    data.remove(name);
  }

  @Override
  public void removeFromArray(String name, int index) {
    validateNotReservedField(name);
    data.withArray(name).remove(index);
  }

  @Override
  public void update(String name, UpdateMode mode, String... values) {
    update(name, mode, v -> setField(name, v), v -> setOrAdd(name, v), values);
  }

  @Override
  public void update(String name, UpdateMode mode, Long... values) {
    update(name, mode, v -> setField(name, v), v -> setOrAdd(name, v), values);
  }

  @Override
  public void update(String name, UpdateMode mode, Integer... values) {
    update(name, mode, v -> setField(name, v), v -> setOrAdd(name, v), values);
  }

  @Override
  public void update(String name, UpdateMode mode, Boolean... values) {
    update(name, mode, v -> setField(name, v), v -> setOrAdd(name, v), values);
  }

  @Override
  public void update(String name, UpdateMode mode, Double... values) {
    update(name, mode, v -> setField(name, v), v -> setOrAdd(name, v), values);
  }

  @Override
  public void update(String name, UpdateMode mode, Instant... values) {
    update(name, mode, v -> setField(name, v), v -> setOrAdd(name, v), values);
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
  @SafeVarargs
  private <T> void update(String name, UpdateMode mode,
                          Consumer<T> setter, Consumer<T> adder, T... values) {

    validateNotReservedField(name);

    if (values.length == 0 || has(name) && mode == UpdateMode.SKIP) {
      return;
    }

    int i = 0;
    if (mode == UpdateMode.OVERWRITE) {
      setter.accept(values[0]);
      i = 1;
    }
    for (; i < values.length; i++) {
      adder.accept(values[i]);
    }
  }

  @Override
  public void initializeRunId(String value) {
    if (data.has(RUNID_FIELD)) {
      throw new IllegalStateException();
    }
    if(value == null || value.isEmpty()) {
      throw new IllegalArgumentException();
    }
    data.put(RUNID_FIELD, value);
  }

  @Override
  public void clearRunId() {
    if (data.has(RUNID_FIELD)) {
      data.remove(RUNID_FIELD);
    }
  }

  @Override
  public void setField(String name, String value) {
    validateNotReservedField(name);
    data.put(name, value);
  }

  @Override
  public void setField(String name, Long value) {
    validateNotReservedField(name);
    data.put(name, value);
  }

  @Override
  public void setField(String name, Integer value) {
    validateNotReservedField(name);
    data.put(name, value);
  }

  @Override
  public void setField(String name, Boolean value) {
    validateNotReservedField(name);
    data.put(name, value);
  }

  @Override
  public void setField(String name, Double value) {
    validateNotReservedField(name);
    data.put(name, value);
  }

  @Override
  public void setField(String name, JsonNode value) {
    validateNotReservedField(name);
    data.set(name, value);
  }

  @Override
  public void setField(String name, Instant value) {
    validateNotReservedField(name);
    data.put(name, INSTANT_FORMATTER.apply(value));
  }

  @Override
  public void renameField(String oldName, String newName, UpdateMode mode) {
    validateNotReservedField(oldName, newName);
    JsonNode oldValues = data.get(oldName);
    data.remove(oldName);

    if (has(newName)) {
      switch (mode) {
        case OVERWRITE:
          break; // continue to setting the new value
        case APPEND:
          convertToList(newName);
          if (oldValues.getNodeType() == JsonNodeType.ARRAY) {
            data.withArray(newName).addAll((ArrayNode) oldValues);
          } else {
            data.withArray(newName).add(oldValues);
          }
          // fall through
        case SKIP:
          return;
        default:
          throw new UnsupportedOperationException("switch not implemented for mode: " + mode);
      }
    }

    data.set(newName, oldValues);
  }

  private <T> T getValue(String name, Function<JsonNode, T> converter) {
    if (!data.has(name)) {
      return null;
    }
    JsonNode node;
    if (!isMultiValued(name)) {
      node = data.get(name);

    } else {
      ArrayNode arr = data.withArray(name);
      if (arr.isEmpty()) {
        throw new IllegalArgumentException("Field " + name + " is empty");
      }
      node = arr.get(0);
    }
    return node.isNull() ? null : converter.apply(node);
  }

  private <T> List<T> getValues(String name, Function<JsonNode, T> converter) {
    if (!data.has(name)) {
      return null;
    }

    if (!isMultiValued(name)) {
      return Collections.singletonList(getValue(name, converter));
    }

    ArrayNode array = data.withArray(name);
    List<T> result = new ArrayList<>();
    for (JsonNode node : array) {
      result.add(node.isNull() ? null : converter.apply(node));
    }
    return result;
  }

  @Override
  public String getString(String name) {
    return getValue(name, JsonNode::asText);
  }

  @Override
  public List<String> getStringList(String name) {
    return getValues(name, JsonNode::asText);
  }

  @Override
  public Integer getInt(String name) {
    return getValue(name, JsonNode::asInt);
  }

  @Override
  public List<Integer> getIntList(String name) {
    return getValues(name, JsonNode::asInt);
  }

  @Override
  public Double getDouble(String name) {
    return getValue(name, JsonNode::asDouble);
  }

  @Override
  public List<Double> getDoubleList(String name) {
    return getValues(name, JsonNode::asDouble);
  }

  @Override
  public Boolean getBoolean(String name) {
    return getValue(name, JsonNode::asBoolean);
  }

  @Override
  public List<Boolean> getBooleanList(String name) {
    return getValues(name, JsonNode::asBoolean);
  }

  @Override
  public Long getLong(String name) {
    return getValue(name, JsonNode::asLong);
  }

  @Override
  public List<Long> getLongList(String name) {
    return getValues(name, JsonNode::asLong);
  }

  @Override
  public Instant getInstant(String name) {
    return getValue(name, node -> DATE_PARSER.apply(node.asText()));
  }

  @Override
  public List<Instant> getInstantList(String name) {
    return getValues(name, node -> DATE_PARSER.apply(node.asText()));
  }

  @Override
  public int length(String name) {
    if (!has(name)) {
      return 0;
    } else if (!isMultiValued(name)) {
      return 1;
    } else {
      return data.get(name).size();
    }
  }

  @Override
  public String getId() {
    return getString(ID_FIELD);
  }

  @Override
  public String getRunId() {
    return getString(RUNID_FIELD);
  }

  @Override
  public boolean has(String name) {
    return data.has(name);
  }

  @Override
  public boolean hasNonNull(String name) {
    return data.hasNonNull(name);
  }

  @Override
  public boolean isMultiValued(String name) {
    return data.has(name) && JsonNodeType.ARRAY.equals(data.get(name).getNodeType());
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other instanceof JsonDocument) {
      return data.equals(((JsonDocument) other).data);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return data.hashCode();
  }

  private ArrayNode getFieldArray(String name) {
    validateNotReservedField(name);
    convertToList(name);
    return data.withArray(name);
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

  @Override
  public void addToField(String name, String value) {
    getFieldArray(name).add(value);
  }

  @Override
  public void addToField(String name, Long value) {
    getFieldArray(name).add(value);
  }

  @Override
  public void addToField(String name, Integer value) {
    getFieldArray(name).add(value);
  }

  @Override
  public void addToField(String name, Boolean value) {
    getFieldArray(name).add(value);
  }

  @Override
  public void addToField(String name, Double value) {
    getFieldArray(name).add(value);
  }

  @Override
  public void addToField(String name, Instant value) {
    getFieldArray(name).add(INSTANT_FORMATTER.apply(value));
  }

  @Override
  public void setOrAdd(String name, String value) {
    if (has(name)) {
      addToField(name, value);
    } else {
      setField(name, value);
    }
  }

  @Override
  public void setOrAdd(String name, Long value) {
    if (has(name)) {
      addToField(name, value);
    } else {
      setField(name, value);
    }
  }

  @Override
  public void setOrAdd(String name, Integer value) {
    if (has(name)) {
      addToField(name, value);
    } else {
      setField(name, value);
    }
  }

  @Override
  public void setOrAdd(String name, Boolean value) {
    if (has(name)) {
      addToField(name, value);
    } else {
      setField(name, value);
    }
  }

  @Override
  public void setOrAdd(String name, Double value) {
    if (has(name)) {
      addToField(name, value);
    } else {
      setField(name, value);
    }
  }

  @Override
  public void setOrAdd(String name, Instant value) {
    if (has(name)) {
      addToField(name, value);
    } else {
      setField(name, value);
    }
  }

  @Override
  public void setOrAdd(String name, Document other) throws IllegalArgumentException {

    validateNotReservedField(name);

    if (!has(name)) {
      if (other.has(name)) {
        data.set(name, getData(other).get(name));
      }
    } else {

      convertToList(name);
      ArrayNode currentValues = (ArrayNode) data.get(name);
      JsonNode otherValue = getData(other).get(name);

      if (otherValue.getNodeType() == JsonNodeType.ARRAY) {
        currentValues.addAll((ArrayNode) otherValue);
      } else {
        currentValues.add(otherValue);
      }
    }
  }

  @Override
  public void setOrAddAll(Document other) {
    for (Iterator<String> it = getData(other).fieldNames(); it.hasNext(); ) {
      String name = it.next();
      if (RESERVED_FIELDS.contains(name)) {
        continue;
      }
      setOrAdd(name, other);
    }
  }

  @Override
  public Map<String,Object> asMap() {
    return MAPPER.convertValue(data, TYPE);
  }

  @Override
  public void addChild(Document document) {
    data.withArray(CHILDREN_FIELD).add(getData(document));
  }

  @Override
  public boolean hasChildren() {
    return data.has(CHILDREN_FIELD) && !getChildren().isEmpty();
  }

  @Override
  public List<Document> getChildren() {
    if (!data.has(CHILDREN_FIELD)) {
      return new ArrayList<>();
    }
    ArrayNode node = data.withArray(CHILDREN_FIELD);
    List<Document> children = new ArrayList<>();
    for (Iterator<JsonNode> it = node.elements(); it.hasNext(); ) {
      JsonNode element = it.next();
      try {
        children.add(new JsonDocument((ObjectNode) element.deepCopy()));
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
  public Document deepCopy() {
    try {
      return new JsonDocument(this);
    } catch (DocumentException e) {
      throw new IllegalStateException("Document not copyable", e);
    }
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
  public Set<String> getFieldNames() {
    Set<String> fieldNames = new HashSet<>();
    for(Iterator<String> it = data.fieldNames(); it.hasNext(); ) {
      fieldNames.add(it.next());
    }
    return fieldNames;
  }

  @Override
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

  private static ObjectNode getData(Document other) {
    if (other == null) {
      throw new IllegalStateException("Document is null");
    }
    if (!(other instanceof JsonDocument)) {
      throw new IllegalStateException("Documents are not of the same type");
    }
    return ((JsonDocument) other).data;
  }
}
