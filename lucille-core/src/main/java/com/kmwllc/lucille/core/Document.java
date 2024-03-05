package com.kmwllc.lucille.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import org.apache.kafka.common.protocol.types.Field.Bool;

public interface Document {

  String ID_FIELD = "id";
  String RUNID_FIELD = "run_id";
  String CHILDREN_FIELD = ".children";
  String DROP_FIELD = ".dropped";

  Set<String> RESERVED_FIELDS = new HashSet<>(List.of(ID_FIELD, RUNID_FIELD, CHILDREN_FIELD, DROP_FIELD));

  void removeField(String name);

  void removeFromArray(String name, int index);

  /**
   * Updates the designated field according to the provided UpdateMode.
   *
   * <p>APPEND: the provided values will be appended to the field. OVERWRITE: the provided values
   * will overwrite any current field values SKIP: the provided values will populate the field if
   * the field didn't previously exist; otherwise no change will be made.
   *
   * <p>In all cases the field will be created if it doesn't already exist.
   */
  default void update(String name, UpdateMode mode, String... values) {
    update(name, mode, (v) -> {
      setField(name, (String) v);
    }, (v) -> {
      setOrAdd(name, (String) v);
    }, values);
  }

  default void update(String name, UpdateMode mode, Long... values) {
    update(name, mode, (v) -> {
      setField(name, (Long) v);
    }, (v) -> {
      setOrAdd(name, (Long) v);
    }, values);
  }

  default void update(String name, UpdateMode mode, Integer... values) {
    update(name, mode, (v) -> {
      setField(name, (Integer) v);
    }, (v) -> {
      setOrAdd(name, (Integer) v);
    }, values);
  }

  default void update(String name, UpdateMode mode, Boolean... values) {
    update(name, mode, (v) -> {
      setField(name, (Boolean) v);
    }, (v) -> {
      setOrAdd(name, (Boolean) v);
    }, values);
  }

  default void update(String name, UpdateMode mode, Double... values) {
    update(name, mode, (v) -> {
      setField(name, (Double) v);
    }, (v) -> {
      setOrAdd(name, (Double) v);
    }, values);
  }

  default void update(String name, UpdateMode mode, Float... values) {
    update(name, mode, (v) -> {
      setField(name, (Float) v);
    }, (v) -> {
      setOrAdd(name, (Float) v);
    }, values);
  }

  default void update(String name, UpdateMode mode, Instant... values) {
    update(name, mode, (v) -> {
      setField(name, (Instant) v);
    }, (v) -> {
      setOrAdd(name, (Instant) v);
    }, values);
  }

  default void update(String name, UpdateMode mode, byte[]... values) {
    update(name, mode, (v) -> {
      setField(name, (byte[]) v);
    }, (v) -> {
      setOrAdd(name, (byte[]) v);
    }, values);
  }

  /**
   * @throws IllegalArgumentException if object is not supported*/
  public default void update(String name, UpdateMode mode, Object... values) {
    Object value = values[0];
  
    if (value instanceof String) {
      validateAllSame(values, v -> v instanceof String);
    } else if (value instanceof Long) {
      validateAllSame(values, v -> v instanceof Long);
    } else if (value instanceof Double) {
      validateAllSame(values, v -> v instanceof Double);
    } else if (value instanceof Boolean) {
      validateAllSame(values, v -> v instanceof Boolean);
    } else if (value instanceof Integer) {
      validateAllSame(values, v -> v instanceof Integer);
    } else if (value instanceof Instant) {
      validateAllSame(values, v -> v instanceof Instant);
    } else if (value instanceof byte[]) {
      validateAllSame(values, v -> v instanceof byte[]);
    } else {
      throw new IllegalArgumentException(String.format("Type of Object: %s is not supported", value.toString()));
    }
    update(name, mode, (v) -> {
      setField(name, v);
    }, (v) -> {
        setOrAdd(name, v);
    }, values);
  }

  private void validateAllSame(Object[] values, Predicate<? super Object> pred) {
    if(!Arrays.stream(values).allMatch(pred)) {
      throw new IllegalArgumentException("All Objects need to have the same subtype");
    }
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

    validateFieldNames(name);

    if (values.length == 0) {
      return;
    }

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

  void initializeRunId(String value);

  void clearRunId();

  void setField(String name, String value);

  void setField(String name, Long value);

  void setField(String name, Integer value);

  void setField(String name, Boolean value);

  void setField(String name, Double value);

  void setField(String name, Float value);

  void setField(String name, JsonNode value);

  void setField(String name, Instant value);

  void setField(String name, byte[] value);

  /**
   * @throws IllegalArgumentException if object is not supported*/
  default void setField(String name, Object value) {
    if (value instanceof String) {
      setField(name, (String) value);
    } else if (value instanceof Long) {
      setField(name, (Long) value);
    } else if (value instanceof Double) {
      setField(name, (Double) value);
    } else if (value instanceof Boolean) {
      setField(name, (Boolean) value);
    } else if (value instanceof Integer) {
      setField(name, (Integer) value);
    } else if (value instanceof Instant) {
      setField(name, (Instant) value);
    } else if (value instanceof byte[]) {
      setField(name, (byte[]) value);
    } else {
      throw new IllegalArgumentException(String.format("Type of Object: %s is not supported", value.toString()));
    }
  }

  void renameField(String oldName, String newName, UpdateMode mode);

  /**
   * This will return null in two cases
   * <ol>
   *   <li>If the field is absent</li>
   *   <li>IF the field is present but contains a null</li>
   * </ol>
   * To distinguish between these, you can call has(). Calling getString for a field which is
   * multivalued will return the first value in the list of Strings.
   * @param name The name of the field to get.
   * @return The value of the field, or null if the field is absent or contains a null.
   */
  String getString(String name);

  List<String> getStringList(String name);

  Integer getInt(String name);

  List<Integer> getIntList(String name);

  Double getDouble(String name);

  List<Double> getDoubleList(String name);

  Float getFloat(String name);

  List<Float> getFloatList(String name);

  Boolean getBoolean(String name);

  List<Boolean> getBooleanList(String name);

  Long getLong(String name);

  List<Long> getLongList(String name);

  Instant getInstant(String name);

  byte[] getBytes(String name);

  List<Instant> getInstantList(String name);

  List<byte[]> getBytesList(String name);

  int length(String name);

  String getId();

  String getRunId();

  boolean has(String name);

  boolean hasNonNull(String name);

  boolean isMultiValued(String name);

  void addToField(String name, String value);

  void addToField(String name, Long value);

  void addToField(String name, Integer value);

  void addToField(String name, Boolean value);

  void addToField(String name, Double value);

  void addToField(String name, Float value);

  /**
   * @throws IllegalArgumentException if object is not supported*/
  default void addToField(String name, Object value) {
    if (value instanceof String) {
      setOrAdd(name, (String) value);
    } else if (value instanceof Long) {
      setOrAdd(name, (Long) value);
    } else if (value instanceof Double) {
      setOrAdd(name, (Double) value);
    } else if (value instanceof Boolean) {
      setOrAdd(name, (Boolean) value);
    } else if (value instanceof Integer) {
      setOrAdd(name, (Integer) value);
    } else if (value instanceof Instant) {
      setOrAdd(name, (Instant) value);
    } else if (value instanceof byte[]) {
      setOrAdd(name, (byte[]) value);
    } else {
      throw new IllegalArgumentException(String.format("Type of Object: %s is not supported", value.toString()));
    }
  }

  /**
   * Converts a given date in Instant form to a string according to DateTimeFormatter.ISO_INSTANT,
   * it can then be accessed as a string via getString() or a converted back to an Instant via
   * getInstant().
   *
   * @param name The name of the field to add to
   * @param value The value to add to the field
   */
  void addToField(String name, Instant value);

  void addToField(String name, byte[] value);

  /**
   * Sets the field to the given value if the field is not already present; otherwise adds it to the
   * field.
   *
   * <p>If the field does not already exist and this method is called once, the field will be
   * created as single-valued; if the field already exists and/or this method is called more than
   * once, the field will be converted to a list of values.
   */
  void setOrAdd(String name, String value);

  void setOrAdd(String name, Long value);

  void setOrAdd(String name, Integer value);

  void setOrAdd(String name, Boolean value);

  void setOrAdd(String name, Double value);

  void setOrAdd(String name, Float value);

  default void setOrAdd(String name, Object value) {
    if (value instanceof String) {
      setOrAdd(name, (String) value);
    } else if (value instanceof Long) {
      setOrAdd(name, (Long) value);
    } else if (value instanceof Double) {
      setOrAdd(name, (Double) value);
    } else if (value instanceof Boolean) {
      setOrAdd(name, (Boolean) value);
    } else if (value instanceof Integer) {
      setOrAdd(name, (Integer) value);
    } else if (value instanceof Instant) {
      setOrAdd(name, (Instant) value);
    } else if (value instanceof byte[]) {
      setOrAdd(name, (byte[]) value);
    } else {
      throw new IllegalArgumentException(String.format("Type of Object: %s is not supported", value.toString()));
    }
  }

  /**
   * Adds a given date in Instant form to a document according to DateTimeFormatter.ISO_INSTANT, can
   * then be accessed as a string via getString() or a converted back to an Instant via
   * getInstant().
   *
   * @param name The name of the field set or add to
   * @param value The value to set or add to the field
   */
  void setOrAdd(String name, Instant value);

  void setOrAdd(String name, byte[] value);

  /**
   * Adds a given field from the designated "other" document to the current document. If a field is
   * already present on the current document, the field is converted to a list.
   *
   * @param name the name of the field to add
   * @param other the document to add the field from
   * @throws IllegalArgumentException if this method is called with a reserved field like id
   */
  void setOrAdd(String name, Document other) throws IllegalArgumentException;

  /**
   * Adds all the fields of the designated "other" document to the current document, excluding
   * reserved fields like id. If a field is already present on the current document, the field is
   * converted to a list and the new value is appended.
   */
  void setOrAddAll(Document other);

  Map<String, Object> asMap();

  void addChild(Document document);

  boolean hasChildren();

  List<Document> getChildren();

  Set<String> getFieldNames();

  boolean isDropped();

  void setDropped(boolean status);

  /**
   * A method to remove duplicate values from multivalued fields in a document and place the values
   * into a target field. If the target field is null or the same as the original field, then
   * modification will happen in place.
   *
   * @param fieldName the field to remove duplicate values from
   * @param targetFieldName the field to copy to
   */
  void removeDuplicateValues(String fieldName, String targetFieldName);

  Document deepCopy();

  /**
   * Returns an Iterator that contains only this document.
   */
  default Iterator<Document> iterator() {
    return Collections.singleton(this).iterator();
  }

  static Document create(ObjectNode node) throws DocumentException {
    return new JsonDocument(node);
  }

  static Document create(String id) {
    return new JsonDocument(id);
  }

  static Document create(String id, String runId) {
    return new JsonDocument(id, runId);
  }

  static Document createFromJson(String json) throws DocumentException, JsonProcessingException {
    return createFromJson(json, null);
  }

  static Document createFromJson(String json, UnaryOperator<String> idUpdater) throws DocumentException, JsonProcessingException {
    return JsonDocument.fromJsonString(json, idUpdater);
  }

  default void validateFieldNames(String... names) throws IllegalArgumentException {
    if (names == null) {
      throw new IllegalArgumentException("expecting string parameters");
    }
    for (String name : names) {
      if (name == null || name.isEmpty()) {
        throw new IllegalArgumentException("Field name cannot be null or empty");
      }
      if (RESERVED_FIELDS.contains(name)) {
        throw new IllegalArgumentException(name + " is a reserved field");
      }
    }
  }

  void removeChildren();
}
