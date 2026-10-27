package com.kmwllc.lucille.core;

import com.dashjoin.jsonata.Jsonata;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.util.FieldFilter;
import java.sql.Timestamp;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
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

  protected static final ObjectMapper MAPPER = createDocumentMapper();
  private static final TypeReference<Map<String, Object>> TYPE = new TypeReference<Map<String, Object>>() {
  };
  private static final Logger log = LoggerFactory.getLogger(JsonDocument.class);

  @JsonValue
  protected ObjectNode data;

  /**
   * Creates the ObjectMapper used to parse JSON into Documents.
   *
   * The mapper is used in several places: it backs {@link #MAPPER} (and therefore {@link #fromJsonString},
   * which {@link Document#createFromJson} dispatches to and which JsonFileHandler calls for each line of
   * a JSON-Lines file), {@link KafkaDocument}'s ConsumerRecord
   * constructor, and KafkaDocumentDeserializer. Every path that turns JSON text into a Document goes
   * through the constraints configured here.
   *
   * Jackson's default StreamReadConstraints cap any single JSON string value at 20,000,000 chars
   * (StreamReadConstraints.DEFAULT_MAX_STRING_LEN). A Document's byte[] field is serialized as one
   * base64 string of 4 * ceil(n / 3) chars, so any byte[] larger than 15,000,000 bytes (about 14.3 MiB;
   * a 15 MiB array encodes to 20,971,520 chars) is written without complaint by KafkaDocumentSerializer,
   * accepted by the broker, and then rejected with a StreamConstraintsException by every consumer that
   * tries to read it back. The constraint exists to bound untrusted input, but a Kafka record is already
   * bounded by the producer's max.request.size (kafka.maxRequestSize) and the broker's
   * message.max.bytes, so a lower limit here only adds a hidden consumer-side cliff below limits the
   * user has already sized. On the non-Kafka paths the caller has already materialized the whole JSON
   * String in memory before the mapper sees it, so the cap could only reject the input after that
   * allocation, not prevent it. maxStringLength is therefore raised to Integer.MAX_VALUE; all other
   * constraints keep their defaults.
   */
  public static ObjectMapper createDocumentMapper() {
    return new ObjectMapper(JsonFactory.builder()
        .streamReadConstraints(StreamReadConstraints.builder().maxStringLength(Integer.MAX_VALUE).build())
        .build());
  }

  /**
   * A copy constructor for {@link Document} that deep copies the ObjectNode and verifies the
   * validity of the document.
   *
   * @param document document to copy
   * @throws DocumentException if the id is invalid, as for {@link #JsonDocument(ObjectNode)}; not expected, since the
   * document being copied already has a valid id
   */
  public JsonDocument(Document document) throws DocumentException {
    this(getData(document).deepCopy());
  }

  /**
   * Creates a new {@link JsonDocument} from a {@link ObjectNode} of key/value pairs. Note: does
   * not defensively copy the given ObjectNode, so it must not be modified after creation
   *
   * @param data the data to be stored in the document
   * @throws DocumentException if the {@link Document#ID_FIELD} is missing, null, empty, blank, or not a string. The id is data
   * here, so a bad one is reported as a checked exception that callers can catch in order to skip the record. See the
   * "Invalid ids" section of {@link Document}.
   */
  public JsonDocument(ObjectNode data) throws DocumentException {

    if (!data.hasNonNull(ID_FIELD)) {
      throw new DocumentException("id is missing or null");
    }

    JsonNode id = data.get(ID_FIELD);
    if (!id.isTextual() || !Document.isValidId(id.asText())) {
      throw new DocumentException("id must be a non-blank string but was "
          + (id.isTextual() ? "an empty or blank string" : "a " + describe(id)));
    }

    this.data = data;
  }

  /**
   * Creates a new {@link JsonDocument} with the given id.
   *
   * <p>The id is a Java argument here, so an invalid one is reported as an unchecked IllegalArgumentException, as
   * invalid field names are. This constructor assumes a valid id: code that obtains ids from external data is
   * responsible for validating them first, with {@link Document#isValidId(String)}, and deciding whether to skip the
   * record or fail. See the "Invalid ids" section of {@link Document}.
   *
   * @param id the document id
   * @throws IllegalArgumentException if the id is null, empty or blank
   */
  public JsonDocument(String id) {
    if (!Document.isValidId(id)) {
      throw new IllegalArgumentException(id == null ? "ID cannot be null" : "ID cannot be empty or blank");
    }
    this.data = MAPPER.createObjectNode();
    this.data.put(ID_FIELD, id);
  }

  public JsonDocument(String id, String runId) {
    this(id);
    this.data.put(RUNID_FIELD, runId);
  }

  public static JsonDocument fromJsonString(String json) throws DocumentException, JsonProcessingException {
    return fromJsonString(json, null, null);
  }

  public static JsonDocument fromJsonString(String json, UnaryOperator<String> idUpdater)
      throws DocumentException, JsonProcessingException {
    return fromJsonString(json, idUpdater, null);
  }

  /**
   * Parses the given JSON text into a {@link JsonDocument}, optionally dropping fields according to the filter and
   * rewriting the id with the updater.
   *
   * @throws JsonProcessingException if the text is not well-formed JSON
   * @throws DocumentException if the JSON is not an object, if its id is invalid as for {@link #JsonDocument(ObjectNode)},
   * or if the id updater returns a null or empty id. All of these concern the data, so they are DocumentExceptions.
   */
  public static JsonDocument fromJsonString(String json, UnaryOperator<String> idUpdater, FieldFilter filter)
      throws DocumentException, JsonProcessingException {

    JsonNode parsed = MAPPER.readTree(json);
    if (!parsed.isObject()) {
      throw new DocumentException("Document JSON must be an object but was " + describe(parsed));
    }
    ObjectNode node = (ObjectNode) parsed;

    if (filter != null && filter.isActive()) {
      Iterator<String> iter = node.fieldNames();
      Set<String> discardedFields = new HashSet<>();
      while (iter.hasNext()) {
        String fieldName = iter.next();
        if (!filter.shouldInclude(fieldName)) {
          discardedFields.add(fieldName);
        }
      }
      node.remove(discardedFields);
    }
    
    JsonDocument doc = new JsonDocument(node);
    if (idUpdater != null) {
      String updatedId = idUpdater.apply(doc.getId());
      if (!Document.isValidId(updatedId)) {
        throw new DocumentException("idUpdater returned a null, empty or blank id for the document with id '" + doc.getId() + "'");
      }
      doc.data.put(ID_FIELD, updatedId);
    }
    return doc;
  }

  /**
   * Describes a parsed JSON value for an error message: its node type, or "empty input" for the MissingNode that
   * readTree returns when there is no content.
   */
  public static String describe(JsonNode node) {
    return node.isMissingNode() ? "empty input" : node.getNodeType().toString().toLowerCase();
  }

  @Override
  public void removeField(String name) {
    validateFieldNames(name);
    data.remove(name);
  }

  @Override
  public void removeFromArray(String name, int index) {
    validateFieldNames(name);
    // withArray() would create an empty array for a missing field and throw for a single-valued one
    if (!isMultiValued(name)) {
      return;
    }
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
    data.remove(RUNID_FIELD);
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
    // a null value is stored as a JSON null, as every other setField overload does
    data.put(name, value == null ? null : DateTimeFormatter.ISO_INSTANT.format(value));
  }

  @Override
  public void setField(String name, byte[] value) {
    validateFieldNames(name);
    data.put(name, value);
  }

  @Override
  public void setField(String name, Date value) {
    setField(name, value == null ? (Instant) null : Instant.ofEpochMilli(value.getTime()));
  }

  @Override
  public void setField(String name, Timestamp value) {
    setField(name, value == null ? (Instant) null : Instant.ofEpochMilli(value.getTime()));
  }

  @Override
  public void renameField(String oldName, String newName, UpdateMode mode) {
    validateFieldNames(oldName, newName);
    if (!data.has(oldName)) {
      // nothing to rename; in particular do not create newName or disturb an existing newName
      return;
    }
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

    return node.isNull() ? null : toFloat(node);
  }

  /**
   * Converts a node to a float. A number is converted with floatValue(). Anything else is coerced the way the other
   * numeric getters (asInt, asLong, asDouble) coerce: a numeric string such as "1.5" is parsed and a boolean becomes 1
   * or 0.
   *
   * <p>Jackson has no asFloat(), and floatValue() does not coerce: it returns 0 for anything that is not a number.
   * Using it for every node would make a field holding "1.5" read as 0.0 through getFloat() but as 1.5 through
   * getDouble(). Nodes that are not numbers therefore go through asDouble(), which coerces them consistently with the
   * other getters.
   *
   * <p>Numbers are not converted that way, because going through a double rounds twice, first to a double and then to a
   * float. When the first rounding lands exactly on the midpoint between two floats, the second can round the wrong
   * way, giving a result one float step from the nearest float. That can happen for a long above 2^53 or a decimal value
   * that is extremely close to such a midpoint; it does not happen for the float, double and int values that make up
   * ordinary data. floatValue() rounds once, straight from the stored value to the nearest float, so every number is
   * converted exactly.
   */
  private static float toFloat(JsonNode node) {
    return node.isNumber() ? node.floatValue() : (float) node.asDouble();
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
      result.add(node.isNull() ? null : toFloat(node));
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
    if (node.isNull()) {
      return null;
    }

    return Instant.from(DateTimeFormatter.ISO_INSTANT.parse(node.asText()));
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
      if (node.isNull()) {
        result.add(null);
        continue;
      }
      result.add(Instant.from(DateTimeFormatter.ISO_INSTANT.parse(node.asText())));
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
        // keep the element's position, as getBytes() returns null for the same error
        log.error("Error accessing byte[] field", e);
        result.add(null);
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

  @Override
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
    if (!isMultiValued(name)) {
      return data.get(name);
    }
    // Multi-valued: return the first element. An empty array has no first element (withArray(name).get(0) would be
    // Java null and NPE the callers' node.isNull() check), so treat it as a null value — the same result the typed
    // accessors give for an explicit null.
    JsonNode first = data.withArray(name).get(0);
    return first == null ? NullNode.getInstance() : first;
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
    // ArrayNode.add((String) null) appends a JSON null, as the other addToField overloads do
    array.add(value == null ? null : DateTimeFormatter.ISO_INSTANT.format(value));
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
    addToField(name, value == null ? (Instant) null : Instant.ofEpochMilli(value.getTime()));
  }

  @Override
  public void addToField(String name, Timestamp value) {
    addToField(name, value == null ? (Instant) null : Instant.ofEpochMilli(value.getTime()));
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

    // checked before anything is modified, so a no-op leaves this document exactly as it was
    JsonNode otherValue = getData(other).get(name);
    if (otherValue == null) {
      return;
    }

    if (!has(name)) {
      data.set(name, unshared(otherValue));
      return;
    }

    convertToList(name);
    ArrayNode currentValues = (ArrayNode) data.get(name);
    if (otherValue.getNodeType() == JsonNodeType.ARRAY) {
      // copied as a whole before appending: when other is this document, otherValue is currentValues itself, and
      // appending to it while iterating it would throw ConcurrentModificationException
      currentValues.addAll((ArrayNode) unshared(otherValue));
    } else {
      currentValues.add(unshared(otherValue));
    }
  }

  /**
   * Returns a node safe to store in this document when it came from another document: an object or array is deep
   * copied, so that later changes to either document cannot show up in the other, and any other scalar is immutable and
   * is shared. The exception is a byte[] value, whose array is shared like everywhere else (see {@link Document}).
   */
  private static JsonNode unshared(JsonNode node) {
    return node.isContainerNode() ? node.deepCopy() : node;
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
  public Map<String, Object> asMap() {
    return MAPPER.convertValue(data, TYPE);
  }

  @Override
  public long getByteSize() {
    return estimateJsonSize(data);
  }

  private static long estimateJsonSize(JsonNode node) {
    switch (node.getNodeType()) {
      case OBJECT: {
        long size = 2; // braces
        boolean first = true;
        for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
          Map.Entry<String, JsonNode> entry = it.next();
          if (!first) {
            size += 1; // comma between entries
          }
          first = false;
          // quoted key plus the colon separator
          size += utf8Length(entry.getKey()) + 3;
          size += estimateJsonSize(entry.getValue());
        }
        return size;
      }
      case ARRAY: {
        long size = 2; // brackets
        boolean first = true;
        for (JsonNode element : node) {
          if (!first) {
            size += 1; // comma between elements
          }
          first = false;
          size += estimateJsonSize(element);
        }
        return size;
      }
      case STRING:
        return utf8Length(node.textValue()) + 2; // content plus quotes
      case NUMBER:
        return node.asText().length();
      case BOOLEAN:
        return node.booleanValue() ? 4 : 5; // true / false
      case NULL:
      case MISSING:
        return 4; // null
      default:
        // any other type, just fall back to serialization
        return node.toString().getBytes(StandardCharsets.UTF_8).length;
    }
  }

  /**
   * Returns the number of bytes the given String requires when encoded as UTF-8. This iterates the
   * String directly and sums the per-character byte count.
   *
   * @param s the String to measure.
   * @return the UTF-8 encoded length of the String in bytes.
   */
  private static int utf8Length(String s) {
    int bytes = 0;
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c <= 0x7F) {
        bytes += 1;
      } else if (c <= 0x7FF) {
        bytes += 2;
      } else if (Character.isHighSurrogate(c) && i + 1 < s.length() && Character.isLowSurrogate(s.charAt(i + 1))) {
        bytes += 4; // a surrogate pair encodes a single 4-byte code point
        i++; // skip the low surrogate we just consumed
      } else {
        bytes += 3;
      }
    }
    return bytes;
  }

  @Override
  public void addChild(Document document) {
    ArrayNode node = data.withArray(CHILDREN_FIELD);
    node.add(getData(document));
  }

  @Override
  public boolean hasChildren() {
    // Answered by inspecting the children array in place. Delegating to getChildren() would deep copy every child
    // only to count them, and hasChildren() is called per document by the indexers and several stages.
    JsonNode children = data.get(CHILDREN_FIELD);
    if (children == null || !children.isArray()) {
      return false;
    }
    for (JsonNode child : children) {
      if (isValidChild(child)) {
        return true;
      }
    }
    return false;
  }

  /**
   * True when the element would be accepted by {@link #getChildren()}: an object with a non-empty string id, the
   * same requirement {@link #JsonDocument(ObjectNode)} enforces. Keeps hasChildren() consistent with getChildren()
   * without constructing any child.
   */
  private static boolean isValidChild(JsonNode child) {
    if (!child.isObject()) {
      return false;
    }
    JsonNode id = child.get(ID_FIELD);
    return id != null && id.isTextual() && Document.isValidId(id.asText());
  }

  /**
   * {@inheritDoc}
   *
   * <p>Each returned child wraps a deep copy of the stored child node. The copy is deliberate: a caller may read,
   * modify, or hold on to the returned Documents without affecting this document, and this document may be modified
   * without affecting them. A caller that wants to change the children replaces them: modify the returned copies,
   * then {@link #removeChildren()} and {@link #addChild(Document)} each one.
   *
   * <p>The one exception is byte[] values: a child's byte arrays are shared with the stored child, so changing the
   * contents of an array in place (as opposed to setting the field to a new array) changes both. See
   * {@link Document} for the rationale.
   */
  @Override
  public List<Document> getChildren() {
    if (!data.has(CHILDREN_FIELD)) {
      return new ArrayList<>();
    }
    ArrayNode node = data.withArray(CHILDREN_FIELD);
    ArrayList<Document> children = new ArrayList<>();
    for (Iterator<JsonNode> it = node.elements(); it.hasNext(); ) {
      JsonNode element = it.next();
      if (!element.isObject()) {
        log.error("Unable to instantiate child Document: child is not an object: {}", element);
        continue;
      }
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

  /**
   * {@inheritDoc}
   *
   * <p>Implemented with {@link ObjectNode#deepCopy()}, which copies objects, arrays and immutable scalars but, for a
   * binary value, returns the same node, so the copy and the original share their byte[] values.
   */
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
    LinkedHashSet<String> fieldNames = new LinkedHashSet<String>();
    // ObjectNode uses a LinkedHashMap internally, so this iteration will follow insertion order into the ObjectNode.
    data.fieldNames().forEachRemaining(fieldNames::add);
    return fieldNames;
  }

  @Override
  public boolean isDropped() {
    // by value, not presence: JSON carrying "___dropped": false must not drop the document
    return data.path(DROP_FIELD).asBoolean(false);
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
  public boolean isSkipped() {
    return data.path(SKIP_FIELD).asBoolean(false);
  }

  @Override
  public void setSkipped(boolean status) {
    if (status) {
      data.put(SKIP_FIELD, true);
    } else {
      data.remove(SKIP_FIELD);
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
    Object transformed = expr.evaluate(asMapWithByteArrayConversion(data));

    if (transformed == null) {
      throw new DocumentException("Transformation must return a Map (JSON object), returned null");
    } else if (!(transformed instanceof Map)) {
      throw new DocumentException("Transformation must return a Map (JSON object), returned " + transformed.getClass());
    }

    // Jsonata-java outputs a Map, converting to ObjectNode so we can update data.
    ObjectNode transformedNode = MAPPER.valueToTree(transformed);

    HashMap<String, JsonNode> reserved = new HashMap<>();
    RESERVED_FIELDS.stream().filter(this::has).forEach(field -> reserved.put(field, data.get(field)));
    for (Map.Entry<String, JsonNode> entry : reserved.entrySet()) {
      if (!entry.getValue().equals(transformedNode.get(entry.getKey()))) {
        throw new DocumentException("The given transformation mutates a reserved field");
      }
    }

    data = transformedNode;
  }

  @Override
  public void transform(Jsonata expr, String sourceField, String destField) throws DocumentException {
    JsonNode sourceNode = getNestedJson(sourceField);
    Object transformed = expr.evaluate(asMapWithByteArrayConversion(sourceNode));

    if (transformed == null) {
      throw new DocumentException("Transformation returned null");
    }

    // Converting the output back to a Jackson-compatible value.
    if (destField == null) {
      destField = sourceField;
    }
    setNestedJson(destField, MAPPER.valueToTree(transformed));
  }

  /**
   * Recursively converts a JsonNode to a Map/List/primitive, including converting binary nodes to base64 strings
   * for compatibility with jsonata-java, which accepts only raw values, Maps, or Lists (not arrays or binary types).
   *
   * This is used in place of this.asMap() for JSONata input: asMap() returns byte[] for binary nodes, which jsonata-java
   * cannot evaluate, whereas this returns the base64 text.
   *
   * @param node the JsonNode to convert
   * @return Object that can be evaluated by jsonata-java
   */
  private Object asMapWithByteArrayConversion(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    } else if (node.isObject()) {
      Map<String, Object> map = new HashMap<>();
      node.fields().forEachRemaining(entry ->
          map.put(entry.getKey(), asMapWithByteArrayConversion(entry.getValue()))
      );
      return map;
    } else if (node.isArray()) {
      List<Object> list = new ArrayList<>();
      node.forEach(element -> list.add(asMapWithByteArrayConversion(element)));
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
    } else {
      return node.toString();
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

  @Override
  public void removeChildren() {
    data.remove(CHILDREN_FIELD);
  }
}
