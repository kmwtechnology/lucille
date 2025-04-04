package com.kmwllc.lucille.core;

import com.api.jsonata4java.expressions.Expressions;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

/**
 * A record from a source system to be passed through an enrichment pipeline and sent to a destination system.
 *
 * Every Document has a String identifier or ID. A Document may also have a "Run ID," which identifies the batch ingest that
 * produced the Document. Documents can be marked as "dropped," meaning they should not be sent to the destination system.
 * Documents may contain nested or enclosed Documents which are known as "children."
 *
 * Documents contain named "fields" which can be single-valued or multi-valued.
 * Supported field types include String, Boolean, Integer, Double, Float, Long, Instant, and byte[].
 * A single field value, and/or the first value in a multi-valued field can be accessed via a getter of the form getString(), etc.
 * A list including a single field value, or all the values in a multi-valued field, be accessed via a getter
 * of the form getStringList(), etc.
 *
 * The Document API supports a variety of setters: setField() methods place a single value in a given field, overwriting
 * any value(s) that were there previously. addToField() methods append a value a given field, converting the field
 * into a list or multi-valued field if it was not already multi-valued. setOrAdd() methods either create the field
 * as single-valued if the field did not exist previously, or append the value to the field if the field already existed.
 * update() methods provide a convient way of toggling between the behaviors of the other setter types: they
 * accept an UpdateMode which specifies the desired behavior and they accept varargs.
 *
 */
public interface Document {

  /* --- NAMES OF RESERVED FIELDS --- */

  String ID_FIELD = "id";
  String RUNID_FIELD = "run_id";
  String CHILDREN_FIELD = ".children";
  String DROP_FIELD = ".dropped";
  Set<String> RESERVED_FIELDS = new HashSet<>(List.of(ID_FIELD, RUNID_FIELD, CHILDREN_FIELD, DROP_FIELD));


  /* --- SINGLE-VALUE GETTERS --- */

  /**
   * Returns the value of the designated field as a String. If the field is multivalued, the first value
   * will be returned.
   *
   * Returns null in two cases: if the field is absent, or if the field is present but contains a null.
   * To distinguish between these cases, use has() to see if the field exists.
   */
  String getString(String name);

  Boolean getBoolean(String name);

  Integer getInt(String name);

  Double getDouble(String name);

  Float getFloat(String name);

  Long getLong(String name);

  Instant getInstant(String name);

  byte[] getBytes(String name);

  JsonNode getJson(String name);

  Date getDate(String name);

  Timestamp getTimestamp(String name);

  /* --- LIST GETTERS --- */

  /**
   * Returns the value of the designated field as a List of Strings. If the field is single-valued, the single value
   * will be placed inside a List and returned, but the field will not be converted to multi-valued.
   *
   * Returns null if the field is absent.
   */
  List<String> getStringList(String name);

  List<Boolean> getBooleanList(String name);

  List<Integer> getIntList(String name);

  List<Double> getDoubleList(String name);

  List<Float> getFloatList(String name);

  List<Long> getLongList(String name);

  List<Instant> getInstantList(String name);

  List<byte[]> getBytesList(String name);

  List<JsonNode> getJsonList(String name);

  List<Date> getDateList(String name);

  List<Timestamp> getTimestampList(String name);

  /* --- SINGLE-VALUE SETTERS --- */

  /**
   * Sets the designated field to the given value, overwriting any value
   * that existed previously, and making the field single-valued.
   *
   * @param name the name of the field you want to set.
   * @param value the value you want to set the field to have.
   */
  void setField(String name, String value);

  void setField(String name, Boolean value);

  void setField(String name, Integer value);

  void setField(String name, Double value);

  void setField(String name, Float value);

  void setField(String name, Long value);

  void setField(String name, Date value);

  void setField(String name, Timestamp value);

  /**
   * Converts the given Instant to a String according to DateTimeFormatter.ISO_INSTANT and
   * places it in the designated field. The value can then be accessed as a String via getString()
   * or a converted back to an Instant via getInstant().
   */
  void setField(String name, Instant value);

  void setField(String name, byte[] value);

  void setField(String name, JsonNode value);

  /**
   * Sets the designated field to the given value, overwriting any value
   * that existed previously, and making the field single-valued.
   * The provided Object value must be one of:
   * String, Long, Double, Boolean, Integer, Float, Instant, byte[], JsonNode, Timestamp, or Date.
   *
   * @param name the name of the field you want to set.
   * @param value the value you want to set the field to have.
   * @throws IllegalArgumentException if value is not of a supported type
   **/
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
    } else if (value instanceof Float) {
      setField(name, (Float) value);
    } else if (value instanceof Instant) {
      setField(name, (Instant) value);
    } else if (value instanceof byte[]) {
      setField(name, (byte[]) value);
    } else if (value instanceof JsonNode) {
      setField(name, (JsonNode) value);
    } else if (value instanceof Timestamp) {
      setField(name, (Timestamp) value);
    } else if (value instanceof Date) {
      setField(name, (Date) value);
    } else {
      throw new IllegalArgumentException(String.format("Type %s is not supported", value.getClass().getName()));
    }
  }


  /* --- LIST ADDERS --- */

  /**
   * Adds the given value to the designated field, converting the field to multivalued (i.e. a list)
   * if it was not multivalued already.
   *
   * @param name the name of the field you want to add the value to.
   * @param value the value you want to add.
   */
  void addToField(String name, String value);

  void addToField(String name, Boolean value);

  void addToField(String name, Integer value);

  void addToField(String name, Double value);

  void addToField(String name, Float value);

  void addToField(String name, Long value);

  void addToField(String name, Instant value);

  void addToField(String name, byte[] value);

  void addToField(String name, JsonNode value);

  void addToField(String name, Date value);

  void addToField(String name, Timestamp value);

  /**
   * Adds the given value to the designated field, converting the field to multivalued (i.e. a list)if it was not multivalued already.
   * The provided Object value must be one of: String, Long, Double, Boolean, Integer, Float, Instant, byte[], JsonNode, Timestamp,
   * or Date.
   *
   * @param name the name of the field you want to add the value to.
   * @param value the value you want to add.
   * @throws IllegalArgumentException if value is not of a supported type
   **/
  default void addToField(String name, Object value) {
    if (value instanceof String) {
      addToField(name, (String) value);
    } else if (value instanceof Long) {
      addToField(name, (Long) value);
    } else if (value instanceof Double) {
      addToField(name, (Double) value);
    } else if (value instanceof Boolean) {
      addToField(name, (Boolean) value);
    } else if (value instanceof Integer) {
      addToField(name, (Integer) value);
    } else if (value instanceof Float) {
      addToField(name, (Float) value);
    } else if (value instanceof Instant) {
      addToField(name, (Instant) value);
    } else if (value instanceof byte[]) {
      addToField(name, (byte[]) value);
    } else if (value instanceof JsonNode) {
      addToField(name, (JsonNode) value);
    } else if (value instanceof Timestamp) {
      addToField(name, (Timestamp) value);
    } else if (value instanceof Date) {
      addToField(name, (Date) value);
    } else {
      throw new IllegalArgumentException(String.format("Type %s is not supported", value.getClass().getName()));
    }
  }


  /* --- SINGLE-VALUE OR LIST ADDERS --- */

  /**
   * Sets the field to the given value if the field is not already present; otherwise, adds the value to the field.
   *
   * <p>If the field does not already exist and this method is called once, the field will be
   * created as single-valued; if the field already exists and/or this method is called more than
   * once, the field will be converted to multivalued (i.e. a list of values).
   *
   * @param name the name of the field you want to set or add a value to.
   * @param value the value you want to set or add to an existing field.
   */
  void setOrAdd(String name, String value);

  void setOrAdd(String name, Boolean value);

  void setOrAdd(String name, Integer value);

  void setOrAdd(String name, Double value);

  void setOrAdd(String name, Float value);

  void setOrAdd(String name, Long value);

  void setOrAdd(String name, Instant value);

  void setOrAdd(String name, byte[] value);

  void setOrAdd(String name, JsonNode value);

  void setOrAdd(String name, Date value);

  void setOrAdd(String name, Timestamp value);

  /**
   * Sets the field to the given value if the field is not already present; otherwise, adds the value to the field.
   *
   * <p>If the field does not already exist and this method is called once, the field will be
   * created as single-valued; if the field already exists and/or this method is called more than
   * once, the field will be converted to multivalued (i.e. a list of values).
   *
   * The provided value must be one of: String, Long, Double, Boolean, Integer, Float, Instant, byte[], JsonNode, Timestamp, Date.
   *
   * @param name the name of the field you want to set or add a value to.
   * @param value the value you want to set or add to an existing field.
   * @throws IllegalArgumentException if the value is not a supported type.
   */
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
    } else if (value instanceof Float) {
      setOrAdd(name, (Float) value);
    } else if (value instanceof Instant) {
      setOrAdd(name, (Instant) value);
    } else if (value instanceof byte[]) {
      setOrAdd(name, (byte[]) value);
    } else if (value instanceof JsonNode) {
      setOrAdd(name, (JsonNode) value);
    } else if (value instanceof Timestamp) {
      setOrAdd(name, (Timestamp) value);
    } else if (value instanceof Date) {
      setOrAdd(name, (Date) value);
    } else {
      throw new IllegalArgumentException(String.format("Type %s is not supported", value.getClass().getName()));
    }
  }

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


  /* --- UPDATERS --- */

  /**
   * Updates the designated field in accordance with the provided UpdateMode.
   * <p> In all cases, the field will be created if it doesn't already exist.
   *
   * @param name the name of the field you want to update.
   * @param mode the UpdateMode you want to use.
   * @param values the value(s) you want the field to have.
   */
  default void update(String name, UpdateMode mode, String... values) {
    update(name, mode, v -> setField(name, (String) v), v -> setOrAdd(name, (String) v), values);
  }

  default void update(String name, UpdateMode mode, Boolean... values) {
    update(name, mode, v -> setField(name, (Boolean) v), v -> setOrAdd(name, (Boolean) v), values);
  }

  default void update(String name, UpdateMode mode, Integer... values) {
    update(name, mode, v -> setField(name, (Integer) v), v -> setOrAdd(name, (Integer) v), values);
  }

  default void update(String name, UpdateMode mode, Double... values) {
    update(name, mode, v -> setField(name, (Double) v), v -> setOrAdd(name, (Double) v), values);
  }

  default void update(String name, UpdateMode mode, Float... values) {
    update(name, mode, v -> setField(name, (Float) v), v -> setOrAdd(name, (Float) v), values);
  }

  default void update(String name, UpdateMode mode, Long... values) {
    update(name, mode, v -> setField(name, (Long) v), v -> setOrAdd(name, (Long) v), values);
  }

  default void update(String name, UpdateMode mode, Instant... values) {
    update(name, mode, v -> setField(name, (Instant) v), v -> setOrAdd(name, (Instant) v), values);
  }

  default void update(String name, UpdateMode mode, byte[]... values) {
    update(name, mode, v -> setField(name, (byte[]) v), v -> setOrAdd(name, (byte[]) v), values);
  }

  default void update(String name, UpdateMode mode, JsonNode... values) {
    update(name, mode, v -> setField(name, (JsonNode) v), v -> setOrAdd(name, (JsonNode) v), values);
  }

  default void update(String name, UpdateMode mode, Date... values) {
    update(name, mode, v -> setField(name, (Date) v), v -> setOrAdd(name, (Date) v), values);
  }

  default void update(String name, UpdateMode mode, Timestamp... values) {
    update(name, mode, v -> setField(name, (Timestamp) v), v -> setOrAdd(name, (Timestamp) v), values);
  }

  /**
   * Updates the designated field according to the provided UpdateMode.
   *
   * <p> The provided values must be of one of: String, Long, Double, Boolean, Integer, Float, Instant, byte[], JsonNode, Timestamp, Date.
   *
   * <p> Note that type uniformity is <b>not</b> enforced.
   *
   * <p> If an unsupported type is encountered, an IllegalArgumentException will be thrown, but any values that had been added to the field
   * will remain in place; that is to say, the partial update will not be reverted if a failure occurs in the middle.
   *
   * @param name the name of the field you want to update.
   * @param mode the UpdateMode you want to use.
   * @param values the value(s) you want the field to have.
   * @throws IllegalArgumentException if any of the values is not of a supported type
   */
  default void update(String name, UpdateMode mode, Object... values) {
    update(name, mode, v -> setField(name, v), (v) -> setOrAdd(name, v), values);
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


  /* --- FIELD UTILITIES --- */

  /**
   * Returns true if the field is present on this document; false otherwise.
   */
  boolean has(String name);

  boolean hasNonNull(String name);

  boolean isMultiValued(String name);

  int length(String name);

  void removeField(String name);

  void removeFromArray(String name, int index);

  void renameField(String oldName, String newName, UpdateMode mode);

  /**
   * A method to remove duplicate values from multivalued fields in a document and place the values
   * into a target field. If the target field is null or the same as the original field, then
   * modification will happen in place.
   *
   * @param fieldName the field to remove duplicate values from
   * @param targetFieldName the field to copy to
   */
  void removeDuplicateValues(String fieldName, String targetFieldName);

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


  /* --- DOCUMENT-LEVEL UTILITIES --- */

  String getId();

  String getRunId();

  void initializeRunId(String value);

  void clearRunId();

  boolean isDropped();

  void setDropped(boolean status);

  Document deepCopy();

  /**
   * Applies a JSONNata expression to this Document (as if it were a JSON object) and replaces this Document with the result.
   *
   * @throws DocumentException if any reserved fields are mutated or if the result is not an object (primitive or array)
   * @see <a href="https://github.com/IBM/JSONata4Java">JSONNata implementation</a>
   */
  void transform(Expressions expr) throws DocumentException;

  /**
   * Returns an Iterator that contains only this document.
   */
  default Iterator<Document> iterator() {
    return Collections.singleton(this).iterator();
  }

  Set<String> getFieldNames();

  Map<String, Object> asMap();


  /* --- CHILD HANDLING UTILITIES --- */

  boolean hasChildren();

  List<Document> getChildren();

  void addChild(Document document);

  void removeChildren();


  /* --- CREATORS --- */

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

}