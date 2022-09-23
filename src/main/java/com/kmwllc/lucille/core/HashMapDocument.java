package com.kmwllc.lucille.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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


public class HashMapDocument extends AbstractDocument {

  private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ISO_INSTANT;
  private static final Function<String, Instant> DATE_PARSER =
    str -> Instant.from(DATE_TIME_FORMATTER.parse(str));
  private static final Function<Instant, String> INSTANT_FORMATTER = DATE_TIME_FORMATTER::format;

  protected static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TypeReference<Map<String, Object>> TYPE = new TypeReference<>() {};
  private static final Logger log = LoggerFactory.getLogger(Document.class);

  protected final Map<String, List<Object>> data;
  protected final Map<String, Class<?>> types;

  protected final String id;
  protected String runId;
  protected List<String> errors;
  protected List<Document> children;

  private static String requireString(JsonNode node) throws DocumentException {
    if (!node.isTextual() || node.asText().isEmpty()) {
      throw new DocumentException("Expected non-empty string, got " + node);
    }
    return node.asText();
  }

  private static String updateString(String toUpdate, UnaryOperator<String> updater) {
    return updater == null ? toUpdate : updater.apply(toUpdate);
  }

  private HashMapDocument(ObjectNode data, UnaryOperator<String> idUpdater) throws DocumentException {

    if (data.getNodeType() != JsonNodeType.OBJECT) {
      throw new DocumentException("data is not an object");
    }

    if (!data.hasNonNull(ID_FIELD)) {
      throw new DocumentException("id is missing");
    }

    this.id = updateString(requireString(data.get(ID_FIELD)), idUpdater);
    this.data = new HashMap<>();
    this.types = new HashMap<>();


    // todo remove
    Map<String, Object> result = MAPPER.convertValue(data, TYPE);
    System.out.println(result);

    for(Iterator<Map.Entry<String, JsonNode>> it = data.fields(); it.hasNext();) {
      Map.Entry<String, JsonNode> entry = it.next();
      String key = entry.getKey();
      JsonNode node = entry.getValue();

      switch (key) {
        case ID_FIELD:
          break;
        case RUNID_FIELD:
          this.runId = requireString(node);
          break;
        case CHILDREN_FIELD:
          if (node.getNodeType() != JsonNodeType.ARRAY) {
            throw new IllegalArgumentException();
          }
          for (JsonNode child: node) {
            addChild(new HashMapDocument((ObjectNode) child, idUpdater));
          }
          break;
        default:
          addNodeValueToField(key, node);
      }
    }
  }

  public void addNodeValueToField(String name, JsonNode node) {
    switch (node.getNodeType()) {
      case STRING:
        addToField(name, node.asText());
        break;
        case NUMBER:
          if (node.isInt()) {
            addToField(name, node.asInt());
          }
          else if (node.isDouble()) {
            addToField(name, node.asDouble());
          }
          else {
            throw new UnsupportedOperationException("Unsupported number type: " + node);
          }
          break;
      case NULL:
        addToFieldList(name, null);
        break;
      case OBJECT:
        addToFieldList(name, node);
        break;
      case ARRAY:
        data.putIfAbsent(name, new ArrayList<>());
        for (JsonNode item: node) {
          addNodeValueToField(name, item);
        }
        break;
      default:
        throw new UnsupportedOperationException("Unsupported type " + node.getNodeType());
    }
  }

  public HashMapDocument(ObjectNode data) throws DocumentException {
    this(data, null);
  }

  public HashMapDocument(String id) {
    if (id == null) {
      throw new NullPointerException("ID cannot be null");
    }
    this.id = id;
    this.data = new HashMap<>();
    this.types = new HashMap<>();
  }

  public HashMapDocument(String id, String runId) {
    this(id);
    if (runId == null) {
      throw new NullPointerException("Run ID cannot be null");
    }
    this.runId = runId;
  }

//   todo review parameters / interface methods
  private HashMapDocument(String id, String runId, List<Document> children, List<String> errors,
                          Map<String, List<Object>> data, Map<String, Class<?>> types) {

    this.id = id;
    this.runId = runId;
    this.children = children;
    this.errors = errors;
    this.types = types;
    this.data = new HashMap<>();

    for (Map.Entry<String, List<Object>> entry: data.entrySet()) {
      this.data.put(entry.getKey(), new ArrayList<>(entry.getValue()));
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

  private void validateNotReservedField(String name) throws IllegalArgumentException {

    if (name == null) {
      throw new IllegalArgumentException("Field name cannot be null");
    }

    if (RESERVED_FIELDS.contains(name)) {
      throw new IllegalArgumentException(name + " is a reserved field");
    }
  }

  @Override
  public void removeField(String name) {
    validateNotReservedField(name);
    data.remove(name);
  }

  @Override
  public void removeFromArray(String name, int index) {
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
    if (runId != null) {
      throw new IllegalStateException();
    }
    runId = value;
  }

  @Override
  public void clearRunId() {
    if (runId != null) {
      runId = null;
    }
  }

  private <T> void setGenericField(String name, T value) {
    validateNotReservedField(name);

    if (value != null) {
      types.putIfAbsent(name, value.getClass());

      if (types.get(name) != value.getClass()) {
        throw new UnsupportedOperationException();
      }
    }

    // need to do it explicitly to handle adding null
    List<Object> list = new ArrayList<>();
    list.add(value);
    data.put(name, list);
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

  private void addAll(String name, List<Object> values) {
    validateNotReservedField(name);
    if (values == null) {
      throw new IllegalArgumentException("values cannot be null");
    }
    data.putIfAbsent(name, new ArrayList<>());
    data.get(name).addAll(values);
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
          addAll(newName, oldValues);
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

    if (data.get(name) == null) {
      return null;
    }

    List<Object> list = data.get(name);
    if (list.isEmpty()) {
      throw new UnsupportedOperationException();
    }
    return list.get(0);
  }

  private <T> T getSingleValue(String name, Function<Object, T> converter) {
    switch (name) {
      case ID_FIELD:
        return converter.apply(id);
      case RUNID_FIELD:
        return converter.apply(runId);
      default:
        Object node = getSingleNode(name);
        return node == null ? null : converter.apply(node);
    }
  }

  private <T> List<T> getValueList(String name, Function<Object, T> converter) {
    if (!has(name)) {
      return null;
    }

    List<T> result = new ArrayList<>();

    if (data.get(name) == null) {
      result.add(null);
      return result;
    }


    for (Object node : data.get(name)) {
      result.add(node == null ? null : converter.apply(node));
    }
    return result;
  }

  // todo might negatively impact performance, can create static fields for each type
  @Override
  public String getString(String name) {
    return getSingleValue(name, String::valueOf);
  }

  @Override
  public List<String> getStringList(String name) {
//    return getValueList(name, value -> (String) value);

    // todo review this
    return getValueList(name, value -> {
      if (value.getClass() == Integer.class) {
        return String.valueOf(value);
      }
      return (String) value;
    });
  }

  @Override
  public Integer getInt(String name) {
    return getSingleValue(name, value -> (Integer) value);
  }

  @Override
  public List<Integer> getIntList(String name) {
    return getValueList(name, value -> {
      if (value.getClass() == String.class) {
        return Integer.valueOf((String) value);
      }
      return (Integer) value;
    });
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
    switch (name) {
      case ID_FIELD:
        return id != null;
      case RUNID_FIELD:
        return runId != null;
      case CHILDREN_FIELD:
        return children != null;
      default:
        return data.containsKey(name);
    }
  }

  @Override
  public boolean hasNonNull(String name) {

    switch (name) {
      case ID_FIELD:
        return id != null;
      case RUNID_FIELD:
        return runId != null;
      case CHILDREN_FIELD:
        return children != null && children.stream().anyMatch(Objects::nonNull);
      default:
        return data.containsKey(name) && data.get(name) != null;
    }
  }

  @Override
  public boolean isMultiValued(String name) {

    if (!RESERVED_FIELDS.contains(name) && !has(name)) {
      return false;
    }
    List<Object> values = data.get(name);
    if (values == null) {
      return false;
    }
    return values.size() > 1;
  }

  private <T> void addToFieldList(String name, T value) {
    validateNotReservedField(name);

    // todo review this
    if (value == null) {
      if (data.containsKey(name)) {
        data.get(name).add(null);
      } else {
        data.put(name, null);
      }
      return;
    }

    data.putIfAbsent(name, new ArrayList<>());


    // todo is there a better way to do this? testGetDoublesMultiValued fails otherwise

    if (types.containsKey(name) && types.get(name) != value.getClass()) {

      if (value.getClass() == Integer.class && types.get(name) == Double.class) {
        data.get(name).add(((Integer) value).doubleValue());
      }
      else {
        throw new UnsupportedOperationException();
      }

    } else {
      data.get(name).add(value);
    }
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

    validateNotReservedField(name);

    if (!(other instanceof HashMapDocument)) {
      throw new UnsupportedOperationException();
    }

    if (!has(name)) {
      if (other.has(name)) {
        for (Object value : ((HashMapDocument) other).data.get(name)) {
          addToFieldList(name, value);
        }
      }
    } else {
      addAll(name, ((HashMapDocument) other).data.get(name));
    }
  }

  @Override
  public void setOrAddAll(Document other) {
    if (!(other instanceof HashMapDocument)) {
      throw new UnsupportedOperationException();
    }
    HashMapDocument otherDocument = (HashMapDocument) other;
    for (Map.Entry<String, List<Object>> entry : otherDocument.data.entrySet()) {
      addAll(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public Map<String, Object> asMap() {

    // todo note that this is not defensive

    Map<String, Object> map = new HashMap<>();
    for (String name : data.keySet()) {
      map.put(name, data.get(name));
    }

    if (id != null) {
      map.put(ID_FIELD, id);
    }

    if (runId != null) {
      map.put(RUNID_FIELD, runId);
    }

    if (children != null) {
      map.put(CHILDREN_FIELD, children);
    }

    return map;
  }

  @Override
  public void addChild(Document document) {
    if (children == null) {
      children = new ArrayList<>();
    }
    children.add(document);
  }

  @Override
  public boolean hasChildren() {
    return children != null;
  }

  @Override
  public List<Document> getChildren() {
    if (children == null) {
      return new ArrayList<>();
    }
    return new ArrayList<>(children);
  }

  @Override
  public Set<String> getFieldNames() {
    Set<String> fieldNames = new HashSet<>(data.keySet());
    if (id != null) {
      fieldNames.add(ID_FIELD);
    }
    if (runId != null) {
      fieldNames.add(RUNID_FIELD);
    }
    return fieldNames;
  }

  @Override
  public void removeDuplicateValues(String fieldName, String targetFieldName) {

    if (!has(fieldName)) {
      throw new IllegalArgumentException("Field " + fieldName + " does not exist");
    }

    if (!isMultiValued(fieldName)) {
      return;
    }

    List<Object> values = data.get(fieldName);
    Set<Object> set = new LinkedHashSet<>(values);

    if (targetFieldName == null || fieldName.equals(targetFieldName)) {
      if (set.size() == values.size()) {
        return;
      }
      data.remove(fieldName);
      data.put(fieldName, new ArrayList<>(set));
    } else {
      data.put(targetFieldName, new ArrayList<>(set));
    }
  }

  @Override
  public Document deepCopy() {
//    return new HashMapDocument(this);
    // todo review this
    return new HashMapDocument(id, runId, children, null, data, types);
  }

  private static String wrap(String s) {
    return "\"" + s + "\"";
  }

  private static String jsonEntry(String key, String value) {
    return wrap(key) + ":" + wrap(value);
  }

  @Override
  public String toString() {

    StringBuilder out = new StringBuilder("{");

    if (id != null) {
      out.append(jsonEntry(ID_FIELD, id));
    }

    if (runId != null) {
      out.append(",").append(jsonEntry(RUNID_FIELD, runId));
    }

    if (children != null) {
      out.append(",").append(wrap(CHILDREN_FIELD)).append(":").append("[");
      for (Document child : children) {
        out.append(child.toString()).append(",");
      }
      out.delete(out.length() - 1, out.length()).append("]");
    }

    if (!this.data.isEmpty()) {
      try {
        String json = MAPPER.writeValueAsString(this.data);
        return out.append(",").append(json.substring(1)).toString();
      } catch (JsonProcessingException e) {

        // todo check the instant to string -> testUpdateInstant

        throw new RuntimeException(e);
      }
    }

    return out.append("}").toString();
  }

  @Override
  public boolean equals(Object obj) {

    if (obj == this) {
      return true;
    }

    if (!(obj instanceof HashMapDocument)) {
      return false;
    }

    HashMapDocument other = (HashMapDocument) obj;

    if (id != null && !id.equals(other.id)) {
      return false;
    }

    if (runId != null && !runId.equals(other.runId)) {
      return false;
    }

    if (children != null && !children.equals(other.children)) {
      return false;
    }

    return data.equals(other.data);
  }

  @Override
  public int hashCode() {
    return Objects.hash(data, types, id, runId, errors, children);
  }
}
