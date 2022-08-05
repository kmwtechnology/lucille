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

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

public class HashMapDocument implements Document {

  private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ISO_INSTANT;
  private static final Function<String, Instant> DATE_PARSER =
    str -> Instant.from(DATE_TIME_FORMATTER.parse(str));
  private static final Function<Instant, String> INSTANT_FORMATTER = DATE_TIME_FORMATTER::format;

  protected static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TypeReference<Map<String, Object>> TYPE = new TypeReference<>() {};
  private static final Logger log = LoggerFactory.getLogger(Document.class);

  protected final Map<String, List<Object>> data;

  protected final String id;
  protected String runId;
  protected List<String> errors;
  protected List<Document> children;

  public HashMapDocument(ObjectNode data) throws DocumentException {

    if (!data.hasNonNull(ID_FIELD)) {
      throw new DocumentException("id is missing");
    }

    JsonNode id = data.get(ID_FIELD);
    if (!id.isTextual() || id.asText().isEmpty()) {
      throw new DocumentException("id is present but null or empty or not a string");
    }

    // todo consider whether should keep it
    throw new UnsupportedOperationException("Not implemented yet");
//    this.data = data;
  }

  public HashMapDocument(String id) {
    if (id == null) {
      throw new NullPointerException("ID cannot be null");
    }
    this.data = new HashMap<>();
    this.id = id;
  }

  public HashMapDocument(String id, String runId) {
    this(id);

    if (runId == null) {
      throw new NullPointerException("Run ID cannot be null");
    }

    this.runId = runId;
  }


  // todo implement these
  public static Document fromJsonString(String json)
    throws DocumentException, JsonProcessingException {
    return new HashMapDocument((ObjectNode) MAPPER.readTree(json));
  }

  public static Document fromJsonString(String json, UnaryOperator<String> idUpdater)
    throws DocumentException, JsonProcessingException {

    throw new UnsupportedOperationException("Not implemented yet");
//
//    GenericDocument doc = fromJsonString(json);
//    doc.getData().put(ID_FIELD, idUpdater.apply(doc.getId()));
//    return doc;
  }

  private void validateNotReservedField(String name) throws IllegalArgumentException {
    if (RESERVED_FIELDS.contains(name)) {
      throw new IllegalArgumentException();
    }
  }



  @Override
  public void removeField(String name) {
    // todo should return value ?
    validateNotReservedField(name);
    data.remove(name);
  }

  @Override
  public void removeFromArray(String name, int index) {
    // todo should return value?
    validateNotReservedField(name);
    data.get(name).remove(index);
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
   * <p>Expects two Consumers that invoke setField and addToField respectively on the named field,
   * passing in a provided value.
   *
   * <p>The Consumer / Lambda Expression approach is used here to avoid code duplication between the
   * various update methods. It is not possible to make update() a generic method because ultimately
   * it would need to call one of the specific setField or addToField methods which in turn call
   * data.put(String, String), data.put(String, Long), data.put(String Boolean)
   */
  @SafeVarargs
  private <T> void update(
    String name, UpdateMode mode, Consumer<T> setter, Consumer<T> adder, T... values) {

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
    if (runId != null) {
      throw new IllegalStateException();
    }
    runId = value;
  }

  @Override
  public void clearRunId() {
    if (runId != null) {
      data.remove(RUNID_FIELD);
    }
  }

  private <T> void setGenericField(String name, T value) {
    validateNotReservedField(name);
    data.put(name, Collections.singletonList(value));
  }

  @Override
  public void setField(String name, String value) {
    setGenericField(name, value);
  }

  @Override
  public void setField(String name, Long value) {
    setGenericField(name, value);
  }

  @Override
  public void setField(String name, Integer value) {
    setGenericField(name, value);
  }

  @Override
  public void setField(String name, Boolean value) {
    setGenericField(name, value);
  }

  @Override
  public void setField(String name, Double value) {
    setGenericField(name, value);
  }

  @Override
  public void setField(String name, JsonNode value) {
    setGenericField(name, value);
  }

  @Override
  public void setField(String name, Instant value) {
    setGenericField(name, value);
  }

  @Override
  public void renameField(String oldName, String newName, UpdateMode mode) {
    validateNotReservedField(oldName);
    validateNotReservedField(newName);
    List<Object> oldValues = data.get(oldName);
    data.remove(oldName);

    // todo consider what happens if the fields have different types

    if (has(newName)) {
      switch (mode) {
        case OVERWRITE:
          break; // todo why is there no overwrite?
        case APPEND:
          data.putIfAbsent(newName, new ArrayList<>());
          data.get(newName).addAll(oldValues);
          // fall through
        case SKIP:
          return;
        default:
          throw new UnsupportedOperationException("switch not implemented for mode: " + mode);
      }
    }

    data.put(newName, oldValues);
  }

  @Override
  public ObjectNode getData() {
    throw new UnsupportedOperationException();
  }

  private Object getSingleNode(String name) {
    // todo what iff list is empty ? is it be possible?
    return data.get(name).get(0);
  }

  private <T> T getSingleValue(String name, Function<Object, T> converter) {
    if (!has(name)) {
      return null;
    }
    Object node = getSingleNode(name);
    T applied = converter.apply(node); // todo this is not necessary
    return node == null ? null : applied;
  }

  private <T> List<T> getValueList(String name, Function<Object, T> converter) {
    if (!has(name)) {
      return null;
    }

    if (!isMultiValued(name)) {
      return Collections.singletonList(getSingleValue(name, converter));
    }

    List<T> result = new ArrayList<>();
    for (Object node : data.get(name)) {
      T applied = converter.apply(node);
      result.add(node == null ? null : applied);
    }
    return result;
  }

  // todo might negatively impact performance, can create static fields for each type
  @Override
  public String getString(String name) {
    return getSingleValue(name, value -> (String) value);
  }

  @Override
  public List<String> getStringList(String name) {
    return getValueList(name, value -> (String) value);
  }

  @Override
  public Integer getInt(String name) {
    return getSingleValue(name, value -> (Integer) value);
  }

  @Override
  public List<Integer> getIntList(String name) {
    return getValueList(name, value -> (Integer) value);
  }

  @Override
  public Double getDouble(String name) {
    return getSingleValue(name, value -> (Double) value);
  }

  @Override
  public List<Double> getDoubleList(String name) {
    return getValueList(name, value -> (Double) value);
  }

  @Override
  public Boolean getBoolean(String name) {
    return getSingleValue(name, value -> (Boolean) value);
  }

  @Override
  public List<Boolean> getBooleanList(String name) {
    return getValueList(name, value -> (Boolean) value);
  }

  @Override
  public Long getLong(String name) {
    return getSingleValue(name, value -> (Long) value);
  }

  @Override
  public List<Long> getLongList(String name) {
    return getValueList(name, value -> (Long) value);
  }

  @Override
  public Instant getInstant(String name) {
    return getSingleValue(name, value -> (Instant) value);
  }

  @Override
  public List<Instant> getInstantList(String name) {
    return getValueList(name, value -> (Instant) value);
  }

  @Override
  public int length(String name) {
    if (!has(name)) {
      return 0;
    }

    // todo again here can ever be an empty list?

    return data.get(name).size();
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public String getRunId() {
    return runId;
  }

  @Override
  public boolean has(String name) {
    return data.containsKey(name);
  }

  @Override
  public boolean hasNonNull(String name) {
    return data.get(name) != null;
  }

  @Override
  public boolean isMultiValued(String name) {
    // todo check this
    return has(name); // && data.get(name).size() > 1;
  }

  private <T> void addToFieldList(String name, T value) {
    validateNotReservedField(name);
    data.putIfAbsent(name, new ArrayList<>());
    data.get(name).add(value);
  }

  @Override
  public void addToField(String name, String value) {
    addToFieldList(name, value);
  }

  @Override
  public void addToField(String name, Long value) {
    addToFieldList(name, value);
  }

  @Override
  public void addToField(String name, Integer value) {
    addToFieldList(name, value);
  }

  @Override
  public void addToField(String name, Boolean value) {
    addToFieldList(name, value);
  }

  @Override
  public void addToField(String name, Double value) {
    addToFieldList(name, value);
  }

  @Override
  public void addToField(String name, Instant value) {
    addToFieldList(name, value);
  }

  private <T> void setOrAddToList(String name, T value) {
    if (has(name)) {
      addToFieldList(name, value);
    } else {
      setGenericField(name, value);
    }
  }

  @Override
  public void setOrAdd(String name, String value) {
    setOrAddToList(name, value);
  }

  @Override
  public void setOrAdd(String name, Long value) {
    setOrAddToList(name, value);
  }

  @Override
  public void setOrAdd(String name, Integer value) {
    setOrAddToList(name, value);
  }

  @Override
  public void setOrAdd(String name, Boolean value) {
    setOrAddToList(name, value);
  }

  @Override
  public void setOrAdd(String name, Double value) {
    setOrAddToList(name, value);
  }

  @Override
  public void setOrAdd(String name, Instant value) {
    setOrAddToList(name, value);
  }

  @Override
  public void setOrAdd(String name, Document other) throws IllegalArgumentException {

    throw new UnsupportedOperationException();

//    validateNotReservedField(name);
//
//    if (!has(name)) {
//      if (other.has(name)) {
//        setField(name, other.getField(name));
//        data.set(name, other.getData().get(name));
//      }
//    } else {
//
//      // todo potentially can get the type of data from other and
//
//
//
//      data.get(name).addAll(other.getData().get(name));
//    }
  }

  @Override
  public void setOrAddAll(Document other) {

    throw new UnsupportedOperationException();


//    for (Iterator<String> it = other.getData().fieldNames(); it.hasNext(); ) {
//      String name = it.next();
//      if (RESERVED_FIELDS.contains(name)) {
//        continue;
//      }
//      setOrAdd(name, other);
//    }
  }

  @Override
  public void logError(String description) {

    // todo shouldn't this error
    addToField(ERROR_FIELD, description);
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
  public Document copy() {
    return null;
  }
}
