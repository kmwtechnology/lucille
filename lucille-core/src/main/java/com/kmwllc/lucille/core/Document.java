package com.kmwllc.lucille.core;

import com.dashjoin.jsonata.Jsonata;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.lang.invoke.MethodHandles;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * Document is an interface so that the storage representation stays separate from the API that stages and
 * indexers program against. {@link JsonDocument}, which stores fields in a Jackson ObjectNode, is currently the
 * only implementation. Code outside this package should create Documents through the static factory methods
 * here ({@link #create(String)}, {@link #createFromJson(String)}, etc.) rather than naming an implementation, so
 * that an alternate implementation could be substituted. A new implementation should extend the shared contract
 * tests in DocumentTest (and NodeDocumentTest, if it is JSON-node-backed) in lucille-core's test sources.
 *
 * <p><b>Sharing and copying.</b> Methods that copy a Document, or move values between Documents, produce independent
 * results with one deliberate exception, byte[] values. Large file contents are commonly held in byte[] fields, and
 * copying them on every deepCopy() or getChildren() would add cost to the filtering and Kafka paths for a hazard that
 * rarely arises, so byte[] values are held by reference:
 * <ul>
 *   <li>setField, addToField and setOrAdd with a byte[] store the array itself, not a copy.</li>
 *   <li>getBytes and getBytesList return the stored arrays themselves, not copies.</li>
 *   <li>deepCopy() and getChildren() return Documents that share their byte[] values with the original.</li>
 *   <li>setOrAdd(name, Document) and setOrAddAll share the other Document's byte[] values.</li>
 * </ul>
 * The consequence is that changing the <i>contents</i> of an array in place, whether the caller still holds the array it
 * set or got one back from a getter, changes every Document that shares it. Setting the field to a new array is always
 * safe. A caller that intends to modify an array in place should clone it first. None of the stages in this
 * repository that read a byte[] field modify it in place.
 *
 * <p>The related cases for other values:
 * <ul>
 *   <li>getJson and getJsonList return the Document's own JsonNodes, and setField, addToField and setOrAdd with a
 *   JsonNode store the given node without copying it. Modifying such a node modifies the Document, and a node set on a
 *   Document must not be modified by the caller afterwards.</li>
 *   <li>addChild stores the given child's data without copying it, so the child must not be modified afterwards.
 *   getChildren, by contrast, returns copies.</li>
 *   <li>setOrAdd(name, Document) and setOrAddAll copy objects and arrays from the other Document, so changes to those
 *   in either Document do not show up in the other.</li>
 *   <li>asMap() builds a new Map with new Lists and byte[] values; modifying it never modifies the Document.</li>
 * </ul>
 *
 * <p><b>Invalid ids.</b> Every Document has a valid String id, one that is not null, empty or only whitespace (see
 * {@link #isValidId(String)}). Which exception reports an invalid one depends on where the id came from, not on what is
 * wrong with it:
 * <ul>
 *   <li>An id read from <i>data</i>, meaning JSON text, an ObjectNode or a Kafka record, is checked: a missing, null,
 *   empty, blank or non-string id is a DocumentException, as is JSON that is not an object, or an id updater that
 *   returns an invalid id. A bad record is an expected possibility with external data, so callers can catch this to skip the
 *   record and continue. Malformed JSON text is a JsonProcessingException, and a Kafka deserializer wraps either in the
 *   SerializationException that Kafka requires, with the original as its cause.</li>
 *   <li>An id passed as a Java <i>argument</i>, as in create(String), is reported with an unchecked
 *   IllegalArgumentException, as an invalid field name is. These factories assume a valid id. Code that obtains ids
 *   from external data is responsible for validating them first, with {@link #isValidId(String)}, and deciding
 *   whether to skip the record or fail; it should not rely on the exception. Null, empty and blank ids are all
 *   rejected, in line with the ObjectNode and JSON paths.</li>
 *   <li>Changing the id of an existing Document is never allowed. Every mutator, including the nested JSON and rename
 *   methods and the update methods, throws IllegalArgumentException for a reserved field name.</li>
 * </ul>
 * In short, a DocumentException means that data being read contained a bad id, and an IllegalArgumentException means
 * that Java code supplied an unacceptable value.
 */
public interface Document {

  Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /* --- NAMES OF RESERVED FIELDS --- */

  String ID_FIELD = "id";
  String RUNID_FIELD = "run_id";
  String CHILDREN_FIELD = "___children";
  String DROP_FIELD = "___dropped";
  String SKIP_FIELD = "___skipped";
  Set<String> RESERVED_FIELDS = new HashSet<>(List.of(ID_FIELD, RUNID_FIELD, CHILDREN_FIELD, DROP_FIELD, SKIP_FIELD));


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

  /**
   * Returns the value of the designated field as a byte[]. The array returned is the one stored in the Document, not a
   * copy; see the class description before modifying its contents.
   */
  byte[] getBytes(String name);

  /**
   * Returns the value of the designated field as a JsonNode. The node returned is the Document's own, not a copy, so
   * modifying it modifies the Document.
   */
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

  /**
   * Returns the values of the designated field as byte arrays. The arrays are the ones stored in the Document, not
   * copies; see the class description before modifying their contents.
   */
  List<byte[]> getBytesList(String name);

  /**
   * Returns the values of the designated field as JsonNodes. The nodes are the Document's own, not copies, so modifying
   * them modifies the Document.
   */
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

  /**
   * Sets the designated field to the given array. The array itself is stored, not a copy, so the caller must not modify
   * its contents afterwards; see the class description.
   */
  void setField(String name, byte[] value);

  /**
   * Sets the designated field to the given node. The node itself is stored, not a copy, so the caller must not modify
   * it afterwards.
   */
  void setField(String name, JsonNode value);

  /**
   * Sets the designated field to the given value, overwriting any value
   * that existed previously, and making the field single-valued.
   * The provided Object value must be one of:
   * String, Long, Double, Boolean, Integer, Float, Instant, byte[], JsonNode, Timestamp, or Date, or null, which
   * stores a JSON null (as every typed setter does for a null argument).
   *
   * @param name the name of the field you want to set.
   * @param value the value you want to set the field to have.
   * @throws IllegalArgumentException if value is not of a supported type
   **/
  default void setField(String name, Object value) {
    if (value == null) {
      // stored as a JSON null, as the typed overloads do for a null argument
      setField(name, (String) value);
    } else if (value instanceof String stringValue) {
      setField(name, stringValue);
    } else if (value instanceof Long longValue) {
      setField(name, longValue);
    } else if (value instanceof Double doubleValue) {
      setField(name, doubleValue);
    } else if (value instanceof Boolean booleanValue) {
      setField(name, booleanValue);
    } else if (value instanceof Integer intValue) {
      setField(name, intValue);
    } else if (value instanceof Float floatValue) {
      setField(name, floatValue);
    } else if (value instanceof Instant instant) {
      setField(name, instant);
    } else if (value instanceof byte[] bytes) {
      setField(name, bytes);
    } else if (value instanceof JsonNode node) {
      setField(name, node);
    } else if (value instanceof Timestamp timestamp) {
      setField(name, timestamp);
    } else if (value instanceof Date date) {
      setField(name, date);
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
    if (value == null) {
      // stored as a JSON null, as the typed overloads do for a null argument
      addToField(name, (String) value);
    } else if (value instanceof String stringValue) {
      addToField(name, stringValue);
    } else if (value instanceof Long longValue) {
      addToField(name, longValue);
    } else if (value instanceof Double doubleValue) {
      addToField(name, doubleValue);
    } else if (value instanceof Boolean booleanValue) {
      addToField(name, booleanValue);
    } else if (value instanceof Integer intValue) {
      addToField(name, intValue);
    } else if (value instanceof Float floatValue) {
      addToField(name, floatValue);
    } else if (value instanceof Instant instant) {
      addToField(name, instant);
    } else if (value instanceof byte[] bytes) {
      addToField(name, bytes);
    } else if (value instanceof JsonNode node) {
      addToField(name, node);
    } else if (value instanceof Timestamp timestamp) {
      addToField(name, timestamp);
    } else if (value instanceof Date date) {
      addToField(name, date);
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
    if (value == null) {
      // stored as a JSON null, as the typed overloads do for a null argument
      setOrAdd(name, (String) value);
    } else if (value instanceof String stringValue) {
      setOrAdd(name, stringValue);
    } else if (value instanceof Long longValue) {
      setOrAdd(name, longValue);
    } else if (value instanceof Double doubleValue) {
      setOrAdd(name, doubleValue);
    } else if (value instanceof Boolean booleanValue) {
      setOrAdd(name, booleanValue);
    } else if (value instanceof Integer intValue) {
      setOrAdd(name, intValue);
    } else if (value instanceof Float floatValue) {
      setOrAdd(name, floatValue);
    } else if (value instanceof Instant instant) {
      setOrAdd(name, instant);
    } else if (value instanceof byte[] bytes) {
      setOrAdd(name, bytes);
    } else if (value instanceof JsonNode node) {
      setOrAdd(name, node);
    } else if (value instanceof Timestamp timestamp) {
      setOrAdd(name, timestamp);
    } else if (value instanceof Date date) {
      setOrAdd(name, date);
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

  /**
   * Indicates whether the document is "skipped." A skipped document flowing through the pipeline will be ignored by downstream stages,
   * but it WILL reach the Indexer and will be indexed as normal.
   * This contrasts with a dropped document, which will be ignored by downstream stages and will NOT be indexed.
   * @return true if this Document should skip processing in stages; false otherwise.
   */
  boolean isSkipped();

  void setSkipped(boolean status);

  /**
   * Returns a copy of this Document that can be modified without affecting this Document, and vice versa, with one
   * exception: byte[] values are shared between the copy and the original rather than copied (including each element
   * of a multi-valued byte[] field, or a byte[] held in a nested JSON value). Fields can be set, added to, replaced and removed freely on either Document; the contents
   * of a byte[] should not be modified in place. See the class description.
   */
  Document deepCopy();

  /**
   * Applies a JSONNata expression to this Document (as if it were a JSON object) and replaces this Document with the result.
   *
   * @throws DocumentException if any reserved fields are mutated or if the result is not an object (primitive or array)
   * @see <a href="https://github.com/dashjoin/jsonata-java">JSONNata implementation</a>
   */
  void transform(Jsonata expr) throws DocumentException;

  /**
   * Applies a JSONNata expression to the value of the source field and places the resulting object in the destination field.
   * @throws DocumentException if the evaluation fails
   */
  void transform(Jsonata expr, String sourceField, String destField) throws DocumentException;

  /**
   * Returns an Iterator that contains only this document.
   */
  default Iterator<Document> iterator() {
    return Collections.singleton(this).iterator();
  }

  /**
   * @return A set of field names found on the Document. Preserves insertion order.
   */
  Set<String> getFieldNames();

  Map<String, Object> asMap();

  /**
   * Returns an estimate, in bytes, of this Document's JSON serialization. The value is intended for
   * relative comparisons (for example, accumulating payload size while batching) and is not guaranteed
   * to match the exact byte count produced by serializing the Document, nor the size the destination
   * search engine uses.
   *
   * @return an estimated size of the Document in bytes.
   */
  long getByteSize();

  /* --- CHILD HANDLING UTILITIES --- */

  /**
   * Returns true if this document has at least one child. Cheap: implementations must answer this without copying or
   * constructing the children, so callers may use it freely as a guard before {@link #getChildren()}.
   */
  boolean hasChildren();

  /**
   * Returns this document's children as independent Documents: copies that neither affect nor are affected by later
   * changes to this document. Callers may read, modify, or retain them freely, except that byte[] values are shared
   * with the stored children (see the class description) and so should not be modified in place.
   *
   * <p>Because they are copies, modifying a returned child does not change this document's children. To change
   * them, modify the returned copies, then call {@link #removeChildren()} and {@link #addChild(Document)} for each.
   * Returns an empty list when there are none.
   */
  List<Document> getChildren();

  /**
   * Adds the given document as a child. The implementation may store the child's own data rather than a copy, so the
   * given document must not be modified after it is added.
   */
  void addChild(Document document);

  void removeChildren();


  /* --- ID VALIDATION --- */

  /**
   * Returns whether the given String is a valid Document id: not null, not empty, and not only whitespace.
   *
   * <p>Every Document must have a valid id, and the factories and constructors enforce this. Connectors and file
   * handlers that build ids from values read from a source system should apply it to the value as read, before any
   * docIdPrefix is added, and skip and report the record when it fails, so that one bad record does not stop the rest of
   * the data from being ingested. Records with a bad id would otherwise either abort the run or share an id with every
   * other such record, silently overwriting one another in the destination.
   *
   * <p>The id is not trimmed: a valid id is used exactly as given. An id assembled from several source values is
   * checked as a whole, so an id with only some blank parts, such as "2_", is valid.
   *
   * <p>Whitespace is as defined by {@link Character#isWhitespace(int)}, so, for example, a non-breaking space does not
   * make an id blank.
   */
  static boolean isValidId(String id) {
    return id != null && !id.isBlank();
  }

  /* --- CREATORS --- */

  // These factories are the designated way to construct a Document outside this package. They are the single
  // place that names the implementation, so swapping it would not require touching callers.

  /**
   * Creates a Document backed by the given node, which is not copied.
   *
   * @throws DocumentException if the node's id is missing, null, empty, blank, or not a string (see "Invalid ids" above)
   */
  static Document create(ObjectNode node) throws DocumentException {
    return new JsonDocument(node);
  }

  /**
   * Creates a Document with the given id.
   *
   * @throws IllegalArgumentException if the id is null, empty or blank (see "Invalid ids" above)
   */
  static Document create(String id) {
    return new JsonDocument(id);
  }

  /**
   * Creates a Document with the given id and run id.
   *
   * @throws IllegalArgumentException if the id is null, empty or blank (see "Invalid ids" above)
   */
  static Document create(String id, String runId) {
    return new JsonDocument(id, runId);
  }

  /**
   * Creates a Document from JSON text.
   *
   * @throws JsonProcessingException if the text is not well-formed JSON
   * @throws DocumentException if the JSON is not an object or its id is invalid (see "Invalid ids" above)
   */
  static Document createFromJson(String json) throws DocumentException, JsonProcessingException {
    return createFromJson(json, null);
  }

  /**
   * Creates a Document from JSON text, replacing its id with the result of the updater when one is given.
   *
   * @throws JsonProcessingException if the text is not well-formed JSON
   * @throws DocumentException if the JSON is not an object, its id is invalid, or the updater returns an invalid
   * id (see "Invalid ids" above)
   */
  static Document createFromJson(String json, UnaryOperator<String> idUpdater) throws DocumentException, JsonProcessingException {
    return JsonDocument.fromJsonString(json, idUpdater);
  }

  /* --- JSON Getters/Setters --- */

  /**
   * Represents a segment of a path to a nested json value within a Document.
   * A Segment may be an index or a name.
   * For example, if the full path the nested json value is a.b.c[4].d[10].e.f[0], the segments would be a, b, c, 4, d, 10, e, f, 0
   */
  class Segment {

    final String name;
    final Integer index;

    public Segment(String name) {
      this.name = name;
      this.index = null;
    }

    public Segment(int index) {
      this.name = null;
      this.index = index;
    }

    public boolean isIndex() {
      return index != null;
    }

    public static List<Segment> parse(String name) {
      List<Segment> segments = new ArrayList<>();
      StringBuffer current = new StringBuffer();
      boolean insideBrackets = false;
      for (int i = 0; i < name.length(); i++) {
        char ch = name.charAt(i);
        if (Character.isWhitespace(ch)) {
          throw new IllegalArgumentException();
        } else if (ch == '[') {
          if (insideBrackets || i == 0) {
            throw new IllegalArgumentException();
          }
          if (!current.isEmpty()) {
            segments.add(new Segment(current.toString()));
            current = new StringBuffer();
          }
          insideBrackets = true;
        } else if (ch == ']') {
          if (!insideBrackets || current.isEmpty()) {
            throw new IllegalArgumentException();
          }
          segments.add(new Segment(Integer.parseInt(current.toString())));
          current = new StringBuffer();
          insideBrackets = false;
        } else if (ch == '.') {
          if (insideBrackets) {
            throw new IllegalArgumentException();
          }
          if (!current.isEmpty()) {
            segments.add(new Segment(current.toString()));
            current = new StringBuffer();
          }
        } else {
          current.append(ch);
        }
      }
      if (insideBrackets) {
        throw new IllegalArgumentException();
      }
      if (!current.isEmpty()) {
        segments.add(new Segment(current.toString()));
      }
      return segments;
    }

    public static String stringify(List<Segment> segments) {
      if (segments == null || segments.isEmpty()) {
        return "";
      }
      if (segments.size() == 1) {
        return segments.get(0).name;
      }
      StringBuffer result = new StringBuffer();
      boolean first = true;
      for (Segment segment : segments) {
        if (segment.isIndex()) {
          result.append("[");
          result.append(segment.index);
          result.append("]");
        } else {
          if (!first) {
            result.append(".");
          }
          result.append(segment.name);
        }
        first = false;
      }
      return result.toString();
    }
  }

  /**
   * Gets a nested JsonNode at a path like "a.b.c.d" where the path is split on '.' and each part is treated as a level of nesting.
   * This also works for nested values that contain a list, such as "a.b[2].c" where 'b' is an ArrayNode. The indices are 0 based.
   *
   * @param name the nested field path to get the JsonNode from
   * @return the JsonNode at the nested path or null if not found
   */
  default JsonNode getNestedJson(String name) {
    return getNestedJson(Segment.parse(name));
  }

  /**
   * Gets a nested JsonNode at the given path segments where each part is treated as a level of nesting.
   * This works for nested values that contain a list, such as ["a","b","2","c"] where 'b' is an ArrayNode. The indices are 0 based.
   *
   * @param segments the nested field path segments to get the JsonNode from
   * @return the JsonNode at the nested path or null if not found
   */
  default JsonNode getNestedJson(List<Segment> segments) {
    if (segments.isEmpty() || !has(segments.get(0).name)) {
      return null;
    }
    JsonNode node = getJson(segments.get(0).name);

    for (Segment segment : segments.subList(1, segments.size())) {
      if (!hasFieldSegment(node, segment)) {
        return null;
      } else {
        node = getFieldSegment(node, segment);
      }
    }

    return node;
  }

  /**
   * Sets a nested JsonNode at a path like "a.b.c.d" where the path is split on '.' and each part is treated as a level of nesting.
   * This also works for nested values that contain a list, such as "a.b[2].c" where 'b' is an ArrayNode. The indices are 0 based.
   *
   * @param name the nested field path to set the JsonNode at
   * @param value the JsonNode to set at the nested path
   */
  default void setNestedJson(String name, JsonNode value) {
    setNestedJson(Segment.parse(name), value);
  }

  /**
   * Sets a nested JsonNode at the given path segments where each part is treated as a level of nesting.
   * This works for nested values that contain a list, such as ["a","b","2","c"] where 'b' is an ArrayNode. The indices are 0 based.
   *
   * @param segments the nested field path segments to set the JsonNode at
   * @param value the JsonNode to set at the nested path
   */
  default void setNestedJson(List<Segment> segments, JsonNode value) {
    if (segments.isEmpty()) {
      return;
    }
    if (segments.size() == 1) {
      setField(segments.get(0).name, value);
      return;
    }

    // if necessary, create a default fresh node where the type is based off of the child field, either ArrayNode or ObjectNode
    JsonNode node = has(segments.get(0).name) ? getJson(segments.get(0).name) : createNewNode(segments.get(1));

    JsonNode currentNode = node;
    for (int i = 1; i < segments.size() - 1; i++) {
      Segment segment = segments.get(i);
      if (hasFieldSegment(currentNode, segment)) {
        currentNode = getFieldSegment(currentNode, segment);
      } else {
        JsonNode childNode = createNewNode(segments.get(i + 1));
        setFieldSegment(currentNode, segment, childNode);
        currentNode = childNode;
      }
    }

    // set last node
    setFieldSegment(currentNode, segments.get(segments.size() - 1), value);

    setField(segments.get(0).name, node);
  }

  default void removeNestedJson(String name) {
    removeNestedJson(Segment.parse(name));
  }

  default void removeNestedJson(List<Segment> segments) {
    if (segments == null || segments.isEmpty()) {
      return;
    }

    if (segments.size() == 1) {
      removeField(segments.get(0).name);
      return;
    }

    JsonNode parent = getNestedJson(segments.subList(0, segments.size() - 1));
    if (parent == null) {
      return;
    }

    Segment last = segments.get(segments.size() - 1);
    if (parent.isObject()) {
      ((ObjectNode) parent).remove(last.name);
    } else if (parent.isArray()) {
      ((ArrayNode) parent).remove(last.index);
    }

    setField(segments.get(0).name, getJson(segments.get(0).name));
  }

  // if the given segment is an integer, it will create a new ArrayNode, otherwise an ObjectNode
  private static JsonNode createNewNode(Segment segment) {
    return segment.isIndex() ? JsonDocument.MAPPER.createArrayNode() : JsonDocument.MAPPER.createObjectNode();
  }

  // sets the given value on the given field segment with either an ObjectNode or ArrayNode
  private static void setFieldSegment(JsonNode node, Segment segment, JsonNode value) {
    if (node.isArray()) {
      if (!segment.isIndex()) {
        throw new IllegalArgumentException();
      }
      // check if index is within the existing array and overwrite if so
      if (segment.index >= 0 && segment.index < node.size()) {
        ((ArrayNode) node).set(segment.index, value);
      } else if (segment.index == node.size()) { // add to end of array (including creating as array if index==0)
        ((ArrayNode) node).add(value);
      } else {
        throw new ArrayIndexOutOfBoundsException("Cannot set index " + segment.index + " on array of size " + node.size());
      }
    } else {
      ((ObjectNode) node).set(segment.name, value);
    }
  }

  // gets the value on the given field segment
  private static JsonNode getFieldSegment(JsonNode node, Segment segment) {
    if (node.isArray()) {
      if (!segment.isIndex()) {
        throw new IllegalArgumentException();
      }
      if (segment.index < 0 || segment.index > node.size() - 1) {
        return null;
      } else {
        return node.get(segment.index);
      }
    } else {
      return node.get(segment.name);
    }
  }

  // checks if the given field segment exists on the given node
  private static boolean hasFieldSegment(JsonNode node, Segment segment) {
    if (node.isArray()) {
      if (!segment.isIndex()) {
        throw new IllegalArgumentException();
      }
      return !(segment.index < 0 || segment.index > node.size() - 1);
    } else {
      return node.has(segment.name);
    }
  }
}