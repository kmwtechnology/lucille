package com.kmwllc.lucille.core;

import com.dashjoin.jsonata.Jsonata;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.sql.Timestamp;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.UnaryOperator;

/**
 * Document implementation that functions as a lightweight wrapper around a piece of JSON.
 * Stores all field data in Jackson ObjectNode.
 *
 */
public class JsonDocument implements Document {

  protected static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TypeReference<Map<String, Object>> TYPE = new TypeReference<Map<String, Object>>() {
  };
  private static final Logger log = LoggerFactory.getLogger(JsonDocument.class);

  @JsonValue
  protected ObjectNode data;

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

  /**
   * Creates a new {@link JsonDocument} from a {@link ObjectNode} of key/value pairs. Note: does
   * not defensively copy the given ObjectNode, so it must not be modified after creation
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

  public JsonDocument(String id) {
    if (id == null) {
      throw new NullPointerException("ID cannot be null");
    }
    this.data = MAPPER.createObjectNode();
    this.data.put(ID_FIELD, id);
  }

  public JsonDocument(String id, String runId) {
    this(id);
    this.data.put(RUNID_FIELD, runId);
  }

  public static JsonDocument fromJsonString(String json) throws DocumentException, JsonProcessingException {
    return fromJsonString(json, null);
  }

  public static JsonDocument fromJsonString(String json, UnaryOperator<String> idUpdater)
      throws DocumentException, JsonProcessingException {
    JsonDocument doc = new JsonDocument((ObjectNode) MAPPER.readTree(json));
    doc.data.put(ID_FIELD, idUpdater == null ? doc.getId() : idUpdater.apply(doc.getId()));
    return doc;
  }

  @Override
  public void removeField(String name) {
    validateFieldNames(name);
    data.remove(name);
  }

  @Override
  public void removeFromArray(String name, int index) {
    validateFieldNames(name);
    data.withArray(name).remove(index);
  }


  @Override
  public void initializeRunId(String value) {
    if (data.has(RUNID_FIELD)) {
      throw new IllegalStateException();
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
    validateFieldNames(name);
    data.put(name, value);
  }

  @Override
  public void setField(String name, Long value) {
    validateFieldNames(name);
    data.put(name, value);
  }

  @Override
  public void setField(String name, Integer value) {
    validateFieldNames(name);
    data.put(name, value);
  }

  @Override
  public void setField(String name, Boolean value) {
    validateFieldNames(name);
    data.put(name, value);
  }

  @Override
  public void setField(String name, Double value) {
    validateFieldNames(name);
    data.put(name, value);
  }

  @Override
  public void setField(String name, Float value) {
    validateFieldNames(name);
    data.put(name, value);
  }

  @Override
  public void setField(String name, JsonNode value) {
    validateFieldNames(name);
    data.set(name, value);
  }

  @Override
  public void setField(String name, Instant value) {
    validateFieldNames(name);
    String instantStr = DateTimeFormatter.ISO_INSTANT.format(value);
    data.put(name, instantStr);
  }

  @Override
  public void setField(String name, byte[] value) {
    validateFieldNames(name);
    data.put(name, value);
  }

  @Override
  public void setField(String name, Date value) {
    Instant instant = Instant.ofEpochMilli(value.getTime());
    setField(name, instant);
  }

  @Override
  public void setField(String name, Timestamp value) {
    Instant instant = Instant.ofEpochMilli(value.getTime());
    setField(name, instant);
  }

  @Override
  public void renameField(String oldName, String newName, UpdateMode mode) {
    validateFieldNames(oldName, newName);
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

    data.set(newName, oldValues);
  }

  @Override
  public String getString(String name) {
    if (!data.has(name)) {
      return null;
    }

    JsonNode node = getSingleNode(name);

    return node.isNull() ? null : node.asText();
  }

  @Override
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

  @Override
  public Integer getInt(String name) {
    if (!data.has(name)) {
      return null;
    }

    JsonNode node = getSingleNode(name);

    return node.isNull() ? null : node.asInt();
  }

  @Override
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

  @Override
  public Double getDouble(String name) {
    if (!data.has(name)) {
      return null;
    }

    JsonNode node = getSingleNode(name);

    return node.isNull() ? null : node.asDouble();
  }

  @Override
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

  @Override
  public Float getFloat(String name) {
    if (!data.has(name)) {
      return null;
    }

    JsonNode node = getSingleNode(name);

    return node.isNull() ? null : node.floatValue();
  }

  @Override
  public List<Float> getFloatList(String name) {
    if (!data.has(name)) {
      return null;
    }

    if (!isMultiValued(name)) {
      return Collections.singletonList(getFloat(name));
    }

    ArrayNode array = data.withArray(name);
    List<Float> result = new ArrayList<>();
    for (JsonNode node : array) {
      result.add(node.isNull() ? null : node.floatValue());
    }
    return result;
  }

  @Override
  public Boolean getBoolean(String name) {
    if (!data.has(name)) {
      return null;
    }

    JsonNode node = getSingleNode(name);

    return node.isNull() ? null : node.asBoolean();
  }

  @Override
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

  @Override
  public Long getLong(String name) {
    if (!data.has(name)) {
      return null;
    }

    JsonNode node = getSingleNode(name);

    return node.isNull() ? null : node.asLong();
  }

  @Override
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

  @Override
  public Instant getInstant(String name) {
    if (!data.has(name)) {
      return null;
    }

    JsonNode node = getSingleNode(name);

    String dateStr = node.asText();
    Instant dateInstant = Instant.from(DateTimeFormatter.ISO_INSTANT.parse(dateStr));
    return node.isNull() ? null : dateInstant;
  }

  @Override
  public byte[] getBytes(String name) {
    if (!data.has(name)) {
      return null;
    }

    JsonNode node = getSingleNode(name);

    try {
      return node.isNull() ? null : node.binaryValue();
    } catch (IOException e) {
      log.error("Error accessing byte[] field", e);
      return null;
    }
  }

  @Override
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

  @Override
  public List<byte[]> getBytesList(String name) {
    if (!data.has(name)) {
      return null;
    }

    if (!isMultiValued(name)) {
      return Collections.singletonList(getBytes(name));
    }

    ArrayNode array = data.withArray(name);
    List<byte[]> result = new ArrayList<>();
    for (JsonNode node : array) {
      try {
        result.add(node.isNull() ? null : node.binaryValue());
      } catch (IOException e) {
        log.error("Error accessing byte[] field", e);
      }
    }
    return result;
  }

  /**
   * Returns a list of JsonNodes.
   *
   * If the field was set to a JsonArray value, a list containing contents of that JsonArray will be returned,
   * as opposed to a List containing the JsonArray itself.
   */
  @Override
  public List<JsonNode> getJsonList(String name) {
    if (!data.has(name)) {
      return null;
    }

    if (!isMultiValued(name)) {
      return Collections.singletonList(getJson(name));
    }

    ArrayNode array = data.withArray(name);
    List<JsonNode> result = new ArrayList<>();
    for (JsonNode node : array) {
      result.add(node);
    }
    return result;
  }

  public JsonNode getJson(String name) {
    if (!data.has(name)) {
      return null;
    }
    // Json is handled differently from other value types
    // we don't call getSingleNode(name) to retrieve the first value from a JsonArray
    // instead, we simply return the internal JsonNode itself, whether it is an array or not
    return data.get(name);
  }

  @Override
  public Date getDate(String name) {
    Instant instant = getInstant(name);
    return instant == null ? null : Date.from(instant);
  }

  @Override
  public List<Date> getDateList(String name) {
    List<Instant> instants = getInstantList(name);
    if (instants == null) {
      return null;
    } else {
      return instants.stream()
          .map(instant -> instant == null ? null : Date.from(instant))
          .collect(Collectors.toList());
    }
  }


  @Override
  public Timestamp getTimestamp(String name) {
    Instant instant = getInstant(name);
    return instant == null ? null : Timestamp.from(instant);
  }

  @Override
  public List<Timestamp> getTimestampList(String name) {
    List<Instant> instants = getInstantList(name);
    if (instants == null) {
      return null;
    } else {
      return instants.stream()
          .map(instant -> instant == null ? null : Timestamp.from(instant))
          .collect(Collectors.toList());
    }
  }

  private JsonNode getSingleNode(String name) {
    return isMultiValued(name) ? data.withArray(name).get(0) : data.get(name);
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
    validateFieldNames(name);
    convertToList(name);
    ArrayNode array = data.withArray(name);
    array.add(value);
  }

  @Override
  public void addToField(String name, Long value) {
    validateFieldNames(name);
    convertToList(name);
    ArrayNode array = data.withArray(name);
    array.add(value);
  }

  @Override
  public void addToField(String name, Integer value) {
    validateFieldNames(name);
    convertToList(name);
    ArrayNode array = data.withArray(name);
    array.add(value);
  }

  @Override
  public void addToField(String name, Boolean value) {
    validateFieldNames(name);
    convertToList(name);
    ArrayNode array = data.withArray(name);
    array.add(value);
  }

  @Override
  public void addToField(String name, Double value) {
    validateFieldNames(name);
    convertToList(name);
    ArrayNode array = data.withArray(name);
    array.add(value);
  }

  @Override
  public void addToField(String name, Float value) {
    validateFieldNames(name);
    convertToList(name);
    ArrayNode array = data.withArray(name);
    array.add(value);
  }

  @Override
  public void addToField(String name, Instant value) {
    validateFieldNames(name);
    convertToList(name);
    ArrayNode array = data.withArray(name);
    String dateStr = DateTimeFormatter.ISO_INSTANT.format(value);
    array.add(dateStr);
  }

  @Override
  public void addToField(String name, byte[] value) {
    validateFieldNames(name);
    convertToList(name);
    ArrayNode array = data.withArray(name);
    array.add(value);
  }

  @Override
  public void addToField(String name, JsonNode value) {
    validateFieldNames(name);
    convertToList(name);
    ArrayNode array = data.withArray(name);
    array.add(value);
  }

  @Override
  public void addToField(String name, Date value) {
    Instant instant = Instant.ofEpochMilli(value.getTime());
    addToField(name, instant);
  }

  @Override
  public void addToField(String name, Timestamp value) {
    Instant instant = Instant.ofEpochMilli(value.getTime());
    addToField(name, instant);
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
  public void setOrAdd(String name, Float value) {
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
  public void setOrAdd(String name, byte[] value) {
    if (has(name)) {
      addToField(name, value);
    } else {
      setField(name, value);
    }
  }

  @Override
  public void setOrAdd(String name, JsonNode value) {
    if (has(name)) {
      addToField(name, value);
    } else {
      setField(name, value);
    }
  }

  @Override
  public void setOrAdd(String name, Date value) {
    if (has(name)) {
      addToField(name, value);
    } else {
      setField(name, value);
    }
  }

  @Override
  public void setOrAdd(String name, Timestamp value) {
    if (has(name)) {
      addToField(name, value);
    } else {
      setField(name, value);
    }
  }

  @Override
  public void setOrAdd(String name, Document other) throws IllegalArgumentException {
    validateFieldNames(name);

    if (!has(name)) {

      if (!other.has(name)) {
        return;
      } else {
        data.set(name, getData(other).get(name));
        return;
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
    for (Iterator<String> it = ((JsonDocument) other).data.fieldNames(); it.hasNext(); ) {
      String name = it.next();
      if (RESERVED_FIELDS.contains(name)) {
        continue;
      }
      setOrAdd(name, other);
    }
  }

  @Override
  public Map<String, Object> asMap() {
    return MAPPER.convertValue(data, TYPE);
  }

  @Override
  public void addChild(Document document) {
    ArrayNode node = data.withArray(CHILDREN_FIELD);
    node.add(getData(document));
  }

  @Override
  public boolean hasChildren() {
    if (!data.has(CHILDREN_FIELD)) {
      return false;
    }
    if (getChildren().isEmpty()) {
      return false;
    }
    return true;
  }

  @Override
  public List<Document> getChildren() {
    if (!data.has(CHILDREN_FIELD)) {
      return new ArrayList<>();
    }
    ArrayNode node = data.withArray(CHILDREN_FIELD);
    ArrayList<Document> children = new ArrayList<>();
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

  @Override
  public Set<String> getFieldNames() {
    Set<String> fieldNames = new HashSet<String>();
    Iterator<String> it = data.fieldNames();
    while (it.hasNext()) {
      String fieldName = it.next();
      fieldNames.add(fieldName);
    }
    return fieldNames;
  }

  @Override
  public boolean isDropped() {
    return data.has(DROP_FIELD);
  }

  @Override
  public void setDropped(boolean status) {
    if (status) {
      data.put(DROP_FIELD, true);
    } else {
      data.remove(DROP_FIELD);
    }
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

  @Override
  public void transform(Jsonata expr) throws DocumentException {
    HashMap<String, JsonNode> reserved = new HashMap<>();
    RESERVED_FIELDS.stream().filter(field -> has(field)).forEach(field -> reserved.put(field, data.get(field)));

    Object transformed;
    try {
      // This is in-line with Jsonata-Java's suggested ways to evaluate expressions on JSON.
      Map<String, Object> dataAsMap = MAPPER.readValue(data.toString(), Map.class);
      transformed = expr.evaluate(dataAsMap);
    } catch (JsonProcessingException e) {
      throw new DocumentException("Error converting JSON to Map: " + e.getMessage());
    }

    if (transformed == null) {
      throw new DocumentException("Transformation must return a Map (JSON object), returned null");
    } else if (!(transformed instanceof Map)) {
      throw new DocumentException("Transformation must return a Map (JSON object), returned " + transformed.getClass());
    }

    // Jsonata-java outputs a Map, converting to ObjectNode so we can update data.
    ObjectNode transformedNode = new ObjectMapper().valueToTree(transformed);

    for (Map.Entry<String, JsonNode> entry : reserved.entrySet()) {
      if (!entry.getValue().equals(transformedNode.get(entry.getKey()))) {
        throw new DocumentException("The given transformation mutates a reserved field");
      }
    }

    data = transformedNode;
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

  @Override
  public void removeChildren() {
   data.remove(CHILDREN_FIELD); 
  }
}
