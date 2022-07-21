package com.kmwllc.lucille.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.time.Instant;

import java.util.*;
import java.util.function.UnaryOperator;

import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class DocumentTest {

  @Test(expected = NullPointerException.class)
  public void testCreateWithoutId1() {
    new Document((String) null);
  }

  @Test(expected = DocumentException.class)
  public void testCreateWithoutId2() throws Exception {
    Document.fromJsonString("{\"field1\":\"val1\"}");
  }

  @Test(expected = DocumentException.class)
  public void testCreateWithoutId3() throws Exception {
    Document.fromJsonString("{\"id\":\"\"}");
  }

  @Test(expected = DocumentException.class)
  public void testIntIdJson() throws Exception {
    Document.fromJsonString("{\"id\":1}");
  }

  @Test(expected = DocumentException.class)
  public void testNullIdJson() throws Exception {
    Document.fromJsonString("{\"id\":null}");
  }

  @Test
  public void testEqualsAndHashcode() throws Exception {

    Document doc1 =
        Document.fromJsonString("{\"id\":\"123\", \"field1\":\"val1\", \"field2\":\"val2\"}");
    Document doc2 =
        Document.fromJsonString("{\"id\":\"123\", \"field2\":\"val2\", \"field1\":\"val1\" }");
    Document doc3 =
        Document.fromJsonString("{\"id\":\"123\", \"field1\":\"val1\", \"field2\":\"val3\"}");

    assertEquals(doc1, doc1);
    assertEquals(doc1, doc2);
    assertEquals(doc2, doc1);
    assertNotEquals(doc3, doc1);
    assertNotEquals(doc1, doc3);
    assertNotEquals(doc1, new Object());
    assertEquals(doc1.hashCode(), doc2.hashCode());

    assertEquals(doc1.hashCode(), doc1.clone().hashCode());
    assertEquals(doc1.clone(), doc1);
    assertEquals(doc1, doc1.clone());

    // hashcodes of unequal objects are not required to be unequal, but if these turned out to be
    // equal
    // it would be a cause for concern
    assertNotEquals(doc1.hashCode(), doc3.hashCode());
  }

  @Test
  public void testLength() {
    Document d = new Document("id");
    d.setField("field1", 1);
    d.setField("field2", 1);
    d.addToField("field2", 2);
    d.addToField("field2", 3);
    assertEquals(1, d.length("field1"));
    assertEquals(3, d.length("field2"));
    assertEquals(0, d.length("field3"));
  }

  @Test(expected = DocumentException.class)
  public void testCreateWithoutId4() throws Exception {
    Document.fromJsonString("{\"id\":null}");
  }

  @Test(expected = DocumentException.class)
  public void testCreateWithoutId5() throws Exception {
    Document.fromJsonString("{\"id\":1}");
  }

  @Test(expected = DocumentException.class)
  public void testCreateWithoutId6() throws Exception {
    new Document(new ObjectMapper().createObjectNode());
  }

  @Test
  public void testCreateFromJsonString() throws Exception {
    // {"id":"123", "field1":"val1", "field2":"val2"}
    Document document =
        Document.fromJsonString("{\"id\":\"123\", \"field1\":\"val1\", \"field2\":\"val2\"}");
    assertEquals("123", document.getString("id"));
    assertEquals("123", document.getId());
    assertEquals("val1", document.getString("field1"));
    assertEquals("val2", document.getString("field2"));
  }

  @Test
  public void testCreateFromJsonStringWithUpdater() throws Exception {
    // {"id":"123", "field1":"val1", "field2":"val2"}
    UnaryOperator<String> updater = s -> "id_" + s;
    Document document =
        Document.fromJsonString(
            "{\"id\":\"123\", \"field1\":\"val1\", \"field2\":\"val2\"}", updater);
    assertEquals("id_123", document.getString("id"));
    assertEquals("id_123", document.getId());
    assertEquals("val1", document.getString("field1"));
    assertEquals("val2", document.getString("field2"));
  }

  @Test
  public void testCreateFromID() {
    Document document = new Document("123");
    assertEquals("123", document.getString("id"));
    assertEquals("123", document.getId());
  }

  @Test
  public void testCreateFromIdAndRunId() {
    Document document = new Document("123", "456");
    assertEquals("123", document.getString("id"));
    assertEquals("123", document.getId());
    assertEquals("456", document.getString("run_id"));
    assertEquals("456", document.getRunId());
  }

  @Test
  public void testIsMultiValued() throws DocumentException, JsonProcessingException {
    Document document =
        Document.fromJsonString(
            ""
                + "{\"id\":\"123\", "
                + "\"null\":null,"
                + "\"single\":\"val1\", "
                + "\"empty_arr\":[],"
                + "\"arr1\":[\"val1\"],"
                + "\"arr2\":[\"val1\", \"val2\"]}");

    assertFalse(document.isMultiValued("id"));
    assertFalse(document.isMultiValued("null"));
    assertFalse(document.isMultiValued("single"));
    assertTrue(document.isMultiValued("empty_arr"));
    assertTrue(document.isMultiValued("arr1"));
    assertTrue(document.isMultiValued("arr2"));

    // not present field
    assertFalse(document.isMultiValued("not_present"));
  }

  @Test
  public void testRemoveReservedField() {
    Document document = new Document("123");
    for (String field : Document.RESERVED_FIELDS) {
      try {
        document.removeField(field);
        fail();
      } catch (IllegalArgumentException e) {
        // expected
      }
    }
  }

  @Test
  public void testRemoveField() throws DocumentException, JsonProcessingException {
    // {"id":"123", "field1":"val1", "field2":"val2"}
    Document document =
        Document.fromJsonString("{\"id\":\"123\", \"field1\":\"val1\", \"field2\":\"val2\"}");
    assertNotNull(document.getString("id"));
    assertNotNull(document.getString("field1"));
    assertNotNull(document.getString("field2"));

    document.removeField("field1");

    assertNotNull(document.getString("id"));
    assertNull(document.getString("field1"));
    assertNotNull(document.getString("field2"));
  }

  @Test
  public void testRemoveFromArray() throws DocumentException, JsonProcessingException {
    // {"id":"123", "field1":"val1", "field2":"val2"}
    Document document = Document.fromJsonString("{\"id\":\"123\", \"array\":[\"val1\", \"val2\"]}");
    assertEquals(List.of("val1", "val2"), document.getStringList("array"));

    document.removeFromArray("array", 1);
    assertEquals(List.of("val1"), document.getStringList("array"));

    document.removeFromArray("array", 0);
    assertEquals(List.of(), document.getStringList("array"));
  }

  @Test
  public void testClearRunId() {
    Document document = new Document("123", "456");
    assertEquals("456", document.getRunId());
    document.clearRunId();
    assertNull(document.getRunId());

    // does nothing
    document.clearRunId();
    assertNull(document.getRunId());
  }

  @Test
  public void testHasNonNull() throws DocumentException, JsonProcessingException {
    // {"id":"123", "field1":"val1", "field2":null}
    Document document =
        Document.fromJsonString("{\"id\":\"123\", \"field1\":\"val1\", \"field2\":null}");
    assertTrue(document.hasNonNull("id"));
    assertTrue(document.hasNonNull("field1"));
    assertFalse(document.hasNonNull("field2"));
  }

  @Test
  public void testSetAndGetField() {
    Document document = new Document("123");
    assertFalse(document.has("field1"));
    document.setField("field1", "val1");
    assertTrue(document.has("field1"));
    assertEquals("val1", document.getString("field1"));
  }

  @Test
  public void testGetUnsetField() {
    Document document = new Document("id");
    assertFalse(document.has("test_field"));
    assertNull(document.getString("test_field"));
    assertNull(document.getStringList("test_field"));
  }

  @Test
  public void testAddToField() {
    Document document = new Document("123");
    assertFalse(document.has("field1"));
    document.addToField("field1", "val1");
    document.addToField("field1", "val2");
    document.addToField("field1", "val3");
    List<String> expected = Arrays.asList("val1", "val2", "val3");
    assertEquals(expected, document.getStringList("field1"));
  }

  @Test
  public void testWriteToField() {
    Document document = new Document("doc");
    assertFalse(document.has("field"));
    document.update("field", UpdateMode.APPEND, "hello there");
    assertEquals("hello there", document.getStringList("field").get(0));
    document.update("field", UpdateMode.APPEND, "some more text", "and some more");
    assertEquals(3, document.getStringList("field").size());
    assertEquals("hello there", document.getStringList("field").get(0));
    assertEquals("some more text", document.getStringList("field").get(1));
    assertEquals("and some more", document.getStringList("field").get(2));
    document.update("field", UpdateMode.OVERWRITE, "this is it now");
    assertEquals(1, document.getStringList("field").size());
    assertEquals("this is it now", document.getString("field"));
    document.update("field", UpdateMode.SKIP, "this won't be written");
    assertEquals(1, document.getStringList("field").size());
    assertEquals("this is it now", document.getString("field"));
  }

  @Test
  public void testGetStringsSingleValued() {
    Document document = new Document("doc");
    document.setField("pets", "dog");
    assertFalse(document.isMultiValued("pets"));
    assertEquals("dog", document.getString("pets"));
    assertEquals(Collections.singletonList("dog"), document.getStringList("pets"));
  }

  @Test
  public void testGetStringsMultiValued() {
    Document document = new Document("doc");
    document.setField("pets", "dog");
    assertFalse(document.isMultiValued("pets"));
    document.addToField("pets", "cat");
    assertTrue(document.isMultiValued("pets"));
    document.addToField("pets", "fish");
    assertEquals(Arrays.asList("dog", "cat", "fish"), document.getStringList("pets"));
  }

  @Test
  public void testGetStringMultivalued() {
    Document document = new Document("doc");
    document.addToField("field1", "val1");
    document.addToField("field1", "val2");
    assertEquals("val1", document.getString("field1"));
  }

  @Test
  public void testGetNullField() throws DocumentException, JsonProcessingException {

    Document document =
        Document.fromJsonString("{\"id\":\"doc\", \"field1\":null, \"field2\":[null]}");

    List<Object> nullList = new ArrayList<>();
    nullList.add(null);

    assertNull(document.getInt("field1"));
    assertEquals(nullList, document.getIntList("field1"));
    assertEquals(nullList, document.getIntList("field2"));

    assertNull(document.getDouble("field1"));
    assertEquals(nullList, document.getDoubleList("field1"));
    assertEquals(nullList, document.getDoubleList("field2"));

    assertNull(document.getBoolean("field1"));
    assertEquals(nullList, document.getBooleanList("field1"));
    assertEquals(nullList, document.getBooleanList("field2"));

    assertNull(document.getLong("field1"));
    assertEquals(nullList, document.getLongList("field1"));
    assertEquals(nullList, document.getLongList("field2"));

    // this fails because tries to parse date from null
    try {
      assertNull(document.getInstant("field1"));
      fail();
    } catch (java.time.format.DateTimeParseException e) {
      // cant parse null
    }
    try {
      assertEquals(nullList, document.getInstantList("field1"));
      fail();
    } catch (java.time.format.DateTimeParseException e) {
      // cant parse null
    }
    try {
      assertEquals(nullList, document.getInstantList("field2"));
      fail();
    } catch (java.time.format.DateTimeParseException e) {
      // cant parse null
    }
  }

  @Test
  public void testGetIntMissing() {
    Document document = new Document("doc");
    assertNull(document.getInt("field1"));
  }

  @Test
  public void testGetIntSingleValued() {
    Document document = new Document("doc");
    document.setField("number", 1);
    assertFalse(document.isMultiValued("number"));
    assertEquals(1, document.getInt("number").intValue());
    assertEquals(Collections.singletonList(1), document.getIntList("number"));
  }

  @Test
  public void testGetIntListMissing() {
    Document document = new Document("doc");
    assertNull(document.getIntList("field1"));
  }

  @Test
  public void testGetIntsMultiValued() {
    Document document = new Document("doc");
    document.setField("pets", 3);
    assertFalse(document.isMultiValued("pets"));
    document.addToField("pets", 2);
    assertTrue(document.isMultiValued("pets"));
    document.addToField("pets", 49);
    assertEquals(Arrays.asList(3, 2, 49), document.getIntList("pets"));
  }

  @Test
  public void testGetIntMultivalued() {
    Document document = new Document("doc");
    document.addToField("field1", 16);
    document.addToField("field1", -38);
    assertEquals(16, document.getInt("field1").intValue());
  }

  @Test
  public void testGetDoubleMissing() {
    Document document = new Document("doc");
    assertNull(document.getDouble("field1"));
  }

  @Test
  public void testGetDoubleSingleValued() {
    Document document = new Document("doc");
    document.setField("double", 1.455);
    assertFalse(document.isMultiValued("double"));
    assertEquals(1.455, document.getDouble("double"), 0);
    assertEquals(Collections.singletonList(1.455), document.getDoubleList("double"));
  }

  @Test
  public void testGetDoubleListMissing() {
    Document document = new Document("doc");
    assertNull(document.getDoubleList("field1"));
  }

  @Test
  public void testGetDoublesMultiValued() {
    Document document = new Document("doc");
    document.setField("gpa", 4.0);
    assertFalse(document.isMultiValued("gpa"));
    document.addToField("gpa", 2);
    assertTrue(document.isMultiValued("gpa"));
    document.addToField("gpa", 2.3);
    assertEquals(Arrays.asList(4.0, 2.0, 2.3), document.getDoubleList("gpa"));
  }

  @Test
  public void testGetDoubleMultivalued() {
    Document document = new Document("doc");
    document.addToField("field1", 16.44);
    document.addToField("field1", -38.91);
    assertEquals(16.44, document.getDouble("field1"), 0);
  }

  @Test
  public void testGetBooleanMissing() {
    Document document = new Document("doc");
    assertNull(document.getBoolean("field1"));
  }

  @Test
  public void testGetBooleanSingleValued() {
    Document document = new Document("doc");
    document.setField("bool", true);
    assertFalse(document.isMultiValued("bool"));
    assertEquals(true, document.getBoolean("bool"));
    assertEquals(Collections.singletonList(true), document.getBooleanList("bool"));
  }

  @Test
  public void testGetBooleanListMissing() {
    Document document = new Document("doc");
    assertNull(document.getBooleanList("field1"));
  }

  @Test
  public void testGetBooleansMultiValued() {
    Document document = new Document("doc");
    document.setField("bools", true);
    assertFalse(document.isMultiValued("bools"));
    document.addToField("bools", false);
    assertTrue(document.isMultiValued("bools"));
    document.addToField("bools", false);
    assertEquals(Arrays.asList(true, false, false), document.getBooleanList("bools"));
  }

  @Test
  public void testGetBooleanMultivalued() {
    Document document = new Document("doc");
    document.addToField("field1", true);
    document.addToField("field1", false);
    assertEquals(true, document.getBoolean("field1"));
  }

  @Test
  public void testGetLongMissing() {
    Document document = new Document("doc");
    assertNull(document.getLong("field1"));
  }

  @Test
  public void testGetLongSingleValued() {
    Document document = new Document("doc");
    document.setField("long", 1000000L);
    assertFalse(document.isMultiValued("long"));
    assertEquals(1000000L, document.getLong("long").longValue());
    assertEquals(Collections.singletonList(1000000L), document.getLongList("long"));
  }

  @Test
  public void testGetLongListMissing() {
    Document document = new Document("doc");
    assertNull(document.getLongList("field1"));
  }

  @Test
  public void testGetLongsMultiValued() {
    Document document = new Document("doc");
    document.setField("longs", 14L);
    assertFalse(document.isMultiValued("longs"));
    document.addToField("longs", 1234L);
    assertTrue(document.isMultiValued("longs"));
    document.addToField("longs", -3L);
    assertEquals(Arrays.asList(14L, 1234L, -3L), document.getLongList("longs"));
  }

  @Test
  public void testGetLongMultivalued() {
    Document document = new Document("doc");
    document.addToField("field1", 3L);
    document.addToField("field1", 1933384L);
    assertEquals(3L, document.getLong("field1").longValue());
  }

  @Test
  public void testGetInstantMissing() {
    Document document = new Document("doc");
    assertNull(document.getInstant("field1"));
  }

  @Test
  public void testGetInstantSingleValued() {
    Document document = new Document("doc");
    document.setField("instant", Instant.ofEpochSecond(10000));
    assertFalse(document.isMultiValued("instant"));
    assertEquals(Instant.ofEpochSecond(10000), document.getInstant("instant"));
    assertEquals(
        Collections.singletonList(Instant.ofEpochSecond(10000)),
        document.getInstantList("instant"));
    assertEquals("1970-01-01T02:46:40Z", document.getString("instant"));
  }

  @Test
  public void testGetInstantListMissing() {
    Document document = new Document("doc");
    assertNull(document.getInstantList("field1"));
  }

  @Test
  public void testGetInstantsMultiValued() {
    Document document = new Document("doc");
    document.setField("instants", Instant.ofEpochSecond(44));
    assertFalse(document.isMultiValued("instants"));
    document.addToField("instants", Instant.ofEpochSecond(1033000));
    assertTrue(document.isMultiValued("instants"));
    document.addToField("instants", Instant.ofEpochSecond(143242));
    assertEquals(
        Arrays.asList(
            Instant.ofEpochSecond(44),
            Instant.ofEpochSecond(1033000),
            Instant.ofEpochSecond(143242)),
        document.getInstantList("instants"));
  }

  @Test
  public void testGetInstantMultivalued() {
    Document document = new Document("doc");
    document.addToField("field1", Instant.ofEpochSecond(44));
    document.addToField("field1", Instant.ofEpochSecond(94));
    assertEquals(Instant.ofEpochSecond(44), document.getInstant("field1"));
  }

  @Test
  public void testChildren() throws Exception {
    Document parent = new Document("parent");
    assertFalse(parent.hasChildren());
    Document child1 = new Document("child1");
    child1.setField("field1", "val1");
    Document child2 = new Document("child2");
    child2.setField("field1", "val1b");
    child1.setField("field2", "val2");
    parent.addChild(child1);
    assertTrue(parent.hasChildren());
    parent.addChild(child2);
    List<Document> children = parent.getChildren();
    assertEquals(2, children.size());
    assertEquals(child1, children.get(0));
    assertEquals(child2, children.get(1));
    Document deserializedParent = Document.fromJsonString(parent.toString());
    assertEquals(parent, Document.fromJsonString(parent.toString()));
    assertTrue(deserializedParent.hasChildren());
    List<Document> deserializedChildren = deserializedParent.getChildren();
    assertEquals(child1, deserializedChildren.get(0));
    assertEquals(child2, deserializedChildren.get(1));
  }

  @Test
  public void testEmptyChildren() throws Exception {
    Document parent = new Document("parent");
    String beforeString = parent.toString();
    List<Document> children = parent.getChildren();
    String afterString = parent.toString();
    // getChildren() should not create a .children field if it didn't already exist,
    // so the json-stringified form of the document should not change after calling getChildren()
    assertEquals(beforeString, afterString);
    assertEquals(0, children.size());
    assertEquals(parent, Document.fromJsonString(parent.toString()));
  }

  @Test
  public void testNullHandling() throws Exception {
    // set a field to null and confirm that we get back a null when we call getString(), not the
    // string "null"
    Document document = new Document("doc");
    document.setField("field1", (String) null);
    assertNull(document.getString("field1"));
    assertFalse(document.isMultiValued("field1"));

    // convert the field to a list, add another null, and confirm that getStringList returns an
    // array with two nulls
    document.addToField("field1", (String) null);
    List<String> field1 = document.getStringList("field1");
    assertNull(field1.get(0));
    assertNull(field1.get(1));
    assertEquals(2, field1.size());

    // stringify the document and recreate it from the string; confirm getStringList still returns
    // array with two nulls
    assertEquals("{\"id\":\"doc\",\"field1\":[null,null]}", document.toString());
    document = Document.fromJsonString(document.toString());
    field1 = document.getStringList("field1");
    assertNull(field1.get(0));
    assertNull(field1.get(1));
    assertEquals(2, field1.size());
    assertTrue(document.isMultiValued("field1"));
  }

  @Test
  public void testRenameField() {
    Document document = new Document("doc");
    document.addToField("initial", "first");
    document.addToField("initial", "second");
    document.renameField("initial", "final", UpdateMode.SKIP);
    List<String> values = document.getStringList("final");
    assertFalse(document.has("initial"));
    assertEquals(List.of("first", "second"), values);
  }

  @Test
  public void testRenameOverwrite() {
    Document document = new Document("document");
    document.setField("initial", "first");
    document.setField("final", "will be repalced");
    assertTrue(document.has("final"));
    document.renameField("initial", "final", UpdateMode.OVERWRITE);
    assertEquals("first", document.getString("final"));
  }

  @Test
  public void testRenameAppend() {
    Document document = new Document("document");
    document.addToField("final", "first");
    document.addToField("final", "second");
    document.addToField("initial", "third");
    document.addToField("initial", "fourth");
    document.renameField("initial", "final", UpdateMode.APPEND);
    assertEquals(4, document.getStringList("final").size());
    assertEquals("first", document.getStringList("final").get(0));
    assertEquals("second", document.getStringList("final").get(1));
    assertEquals("third", document.getStringList("final").get(2));
    assertEquals("fourth", document.getStringList("final").get(3));
  }

  @Test
  public void testRenameFieldSkip() {
    Document document = new Document("doc");
    document.addToField("initial", "first");
    document.addToField("final", "second");
    assertEquals("first", document.getString("initial"));
    assertEquals("second", document.getString("final"));

    document.renameField("initial", "final", UpdateMode.SKIP);

    assertEquals("second", document.getString("final"));
    // in this case, “skip” means “don’t overwrite the destination field, if that field already
    // exists”
    assertFalse(document.has("initial"));
  }

  @Test
  public void testRenameAppendNotArray() throws DocumentException, JsonProcessingException {

    // {"id":"123", "field1":"val1"}
    Document document = Document.fromJsonString("{\"id\":\"123\", \"field1\":\"val1\"}");
    document.addToField("final", "first");
    document.addToField("final", "second");

    assertEquals("val1", document.getString("field1"));
    assertEquals(List.of("first", "second"), document.getStringList("final"));

    document.renameField("field1", "final", UpdateMode.APPEND);

    assertFalse(document.has("field1"));
    assertEquals(List.of("first", "second", "val1"), document.getStringList("final"));
  }

  private static List<Object> toList(Object value) {
    // todo this is a temporary fix, intellij suggests gentrifying
    return (List<Object>) value;
  }

  private static List<Object> valueListFromDocument(Document document, String field) {
    return toList(document.asMap().get(field));
  }

  @Test
  public void testRenamePreservesTypes() {
    Document document = new Document("document");
    document.setField("initial", 5);
    document.addToField("initial", 22);
    document.renameField("initial", "final", UpdateMode.OVERWRITE);
    Map<String, Object> map = document.asMap();
    List<Object> finalValues = toList(map.get("final"));
    assertEquals(5, finalValues.get(0));
    assertNotEquals(5.0, finalValues.get(0));
    assertNotEquals("5", finalValues.get(0));
    assertEquals(22, finalValues.get(1));
  }

  @Test
  public void testUpdateString() {
    Document document = new Document("id1");
    document.update("myStringField", UpdateMode.OVERWRITE, "val1");
    document.update("myStringField", UpdateMode.OVERWRITE, "val2");
    document.update("myStringField", UpdateMode.APPEND, "val3");
    document.update("myStringField", UpdateMode.SKIP, "val4");
    assertEquals(List.of("val2", "val3"), valueListFromDocument(document, "myStringField"));
  }

  @Test
  public void testUpdateInt() {
    Document document = new Document("id1");
    document.update("myIntField", UpdateMode.OVERWRITE, 1);
    document.update("myIntField", UpdateMode.OVERWRITE, 2);
    document.update("myIntField", UpdateMode.APPEND, 3);
    document.update("myIntField", UpdateMode.SKIP, 4);
    assertEquals(List.of(2, 3), valueListFromDocument(document, "myIntField"));
  }

  @Test
  public void testUpdateLong() {
    Document document = new Document("id1");
    document.update("myLongField", UpdateMode.OVERWRITE, 1L);
    document.update("myLongField", UpdateMode.OVERWRITE, 2L);
    document.update("myLongField", UpdateMode.APPEND, 3L);
    document.update("myLongField", UpdateMode.SKIP, 4L);
    assertEquals(List.of(2L, 3L), valueListFromDocument(document, "myLongField"));
  }

  @Test
  public void testUpdateDouble() {
    Document document = new Document("id1");
    document.update("myDoubleField", UpdateMode.OVERWRITE, 1D);
    document.update("myDoubleField", UpdateMode.OVERWRITE, 2D);
    document.update("myDoubleField", UpdateMode.APPEND, 3D);
    document.update("myDoubleField", UpdateMode.SKIP, 4D);
    assertEquals(List.of(2D, 3D), valueListFromDocument(document, "myDoubleField"));
  }

  @Test
  public void testUpdateBoolean() {
    Document document = new Document("id1");
    document.update("myBooleanField", UpdateMode.OVERWRITE, true);
    document.update("myBooleanField", UpdateMode.OVERWRITE, false);
    document.update("myBooleanField", UpdateMode.APPEND, true);
    document.update("myBooleanField", UpdateMode.SKIP, false);
    assertEquals(List.of(false, true), valueListFromDocument(document, "myBooleanField"));
  }

  @Test
  public void testUpdateInstant() {

    Document document = new Document("id1");
    document.update("myInstantField", UpdateMode.OVERWRITE, Instant.ofEpochSecond(1));
    document.update("myInstantField", UpdateMode.OVERWRITE, Instant.ofEpochSecond(2));
    document.update("myInstantField", UpdateMode.APPEND, Instant.ofEpochSecond(3));
    document.update("myInstantField", UpdateMode.SKIP, Instant.ofEpochSecond(4));

    assertEquals(
        List.of(Instant.ofEpochSecond(2), Instant.ofEpochSecond(3)),
        document.getInstantList("myInstantField"));

    // previous version of the test compares strings rather than instances
    //    assertEquals(
    //        Stream.of(Instant.ofEpochSecond(2), Instant.ofEpochSecond(3))
    //            .map(Instant::toString)
    //            .collect(Collectors.toList()),
    //        valueListFromDocument(document, "myInstantField"));
  }

  @Test
  public void testUpdateSingleVersusMultiValued() {
    Document document = new Document("id1");
    document.update("myStringField1", UpdateMode.OVERWRITE, "val1");
    assertFalse(document.isMultiValued("myStringField1"));
    document.update("myStringField1", UpdateMode.OVERWRITE, "val2");
    assertFalse(document.isMultiValued("myStringField1"));
    document.update("myStringField1", UpdateMode.OVERWRITE, "val1", "val2", "val3");
    assertTrue(document.isMultiValued("myStringField1"));
    document.update("myStringField1", UpdateMode.OVERWRITE, "val1");
    assertFalse(document.isMultiValued("myStringField1"));

    // when we call APPEND on a field that doesn't exist, it gets created as a single-valued field
    document.update("myStringField2", UpdateMode.APPEND, "val1");
    assertFalse(document.isMultiValued("myStringField2"));
    // when we call APPEND on a field that already exists, now it becomes multi-valued if it wasn't
    // already
    document.update("myStringField2", UpdateMode.APPEND, "val2");
    assertTrue(document.isMultiValued("myStringField2"));
    document.update("myStringField2", UpdateMode.OVERWRITE, "val3");
    assertFalse(document.isMultiValued("myStringField2"));
  }

  @Test(expected = Exception.class)
  public void testSetDocIdFails() {
    Document document = new Document("id1");
    document.setField(Document.ID_FIELD, "id2");
  }

  @Test(expected = Exception.class)
  public void testAddToDocIdFails() {
    Document document = new Document("id1");
    document.addToField(Document.ID_FIELD, "id2");
  }

  @Test(expected = Exception.class)
  public void testUpdateDocIdFails() {
    Document document = new Document("id1");
    document.update(Document.ID_FIELD, UpdateMode.OVERWRITE, "id2");
  }

  @Test(expected = Exception.class)
  public void testSetRunIdFails() {
    Document document = new Document("id1");
    document.initializeRunId("run_id1");
    document.setField(Document.RUNID_FIELD, "id2");
  }

  @Test(expected = Exception.class)
  public void testReInitializeRunIdFails() {
    Document document = new Document("id1");
    document.initializeRunId("run_id1");
    document.initializeRunId("run_id2");
  }

  @Test
  public void testSetOrAdd() {

    // confirm setOrAdd behaves as expected
    Document document = new Document("id1");
    document.setOrAdd("field1", "value1");
    assertFalse(document.isMultiValued("field1"));
    assertEquals("value1", document.getString("field1"));
    document.setOrAdd("field1", "value2");
    assertTrue(document.isMultiValued("field1"));
    assertEquals("value1", document.getStringList("field1").get(0));
    assertEquals("value2", document.getStringList("field1").get(1));
    assertEquals(2, document.getStringList("field1").size());

    // compare with setField behavior
    Document document2 = new Document("id2");
    document2.setField("field1", "value1");
    assertFalse(document2.isMultiValued("field1"));
    assertEquals("value1", document2.getString("field1"));
    document2.setField("field1", "value2");
    assertFalse(document2.isMultiValued("field1"));
    assertEquals("value2", document2.getString("field1"));

    // compare with addToField behavior
    Document document3 = new Document("id1");
    document3.addToField("field1", "value1");
    assertTrue(document3.isMultiValued("field1"));
    assertEquals("value1", document3.getString("field1"));
    document3.addToField("field1", "value2");
    assertTrue(document3.isMultiValued("field1"));
    assertEquals("value1", document3.getStringList("field1").get(0));
    assertEquals("value2", document3.getStringList("field1").get(1));
    assertEquals(2, document3.getStringList("field1").size());
  }

  @Test
  public void testSetOrAddInstant() {
    Document document = new Document("id1");
    assertFalse(document.has("instant"));

    document.setOrAdd("instant", Instant.ofEpochSecond(10000));
    assertTrue(document.has("instant"));
    assertFalse(document.isMultiValued("instant"));

    document.setOrAdd("instant", Instant.ofEpochSecond(20000));
    assertTrue(document.isMultiValued("instant"));
  }

  @Test
  public void testSetOrAddDocumentSingleValued() {

    Document document = new Document("id1");
    Document otherDoc = new Document("id2");

    document.setOrAdd("newField", otherDoc);
    assertFalse(otherDoc.has("newField"));
    assertFalse(document.has("newField"));

    otherDoc.setOrAdd("newField", "sample");
    document.setOrAdd("newField", otherDoc);
    assertTrue(document.has("newField"));
    assertTrue(otherDoc.has("newField"));
    assertEquals("sample", document.getString("newField"));
  }

  @Test
  public void testSetOrAddDocumentMultiValued() {

    Document document = new Document("id1");
    Document otherDoc = new Document("id2");

    document.setOrAdd("newField", "val0");
    assertFalse(document.isMultiValued("newField"));
    assertEquals("val0", document.getString("newField"));

    otherDoc.setOrAdd("newField", "val1");
    otherDoc.setOrAdd("newField", "val2");
    assertTrue(otherDoc.isMultiValued("newField"));
    assertEquals(List.of("val1", "val2"), otherDoc.getStringList("newField"));

    document.setOrAdd("newField", otherDoc);
    assertTrue(document.isMultiValued("newField"));
    assertEquals(List.of("val0", "val1", "val2"), document.getStringList("newField"));
  }

  @Test
  public void testSetOrAddWithOtherDocument() {

    Document d1 = new Document("id1");
    d1.initializeRunId("run1");
    d1.setField("stringField", "val");
    d1.setField("intField", 1);
    d1.setField("boolField", true);
    d1.setField("doubleField", 2.0);
    d1.setField("longField", 3L);

    Document d2 = d1.clone();
    d1.setOrAddAll(d2);
    d1.setOrAddAll(d2);
    d1.setOrAddAll(d2);

    Document expected = new Document("id1");
    expected.initializeRunId("run1");
    for (int i = 0; i <= 3; i++) {
      expected.setOrAdd("stringField", "val");
      expected.setOrAdd("intField", 1);
      expected.setOrAdd("boolField", true);
      expected.setOrAdd("doubleField", 2.0);
      expected.setOrAdd("longField", 3L);
    }

    assertEquals(expected, d1);
  }

  @Test
  public void testJsonField() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode node = mapper.readTree("{\"a\":1, \"b\":2}");
    Document d = new Document("id1");
    d.setField("myField", node);

    assertEquals(d, Document.fromJsonString(d.toString()));
    assertEquals(d.toString(), Document.fromJsonString(d.toString()).toString());

    JsonNode node2 = mapper.readTree("{\"a\":1, \"b\":3}");
    Document d2 = new Document("id1");
    d2.setField("myField", node2);
    assertNotEquals(d, d2);

    d2.setField("myField", node.deepCopy());
    assertEquals(d, d2);
  }

  @Test
  public void testJsonMultivaluedField() throws Exception {
    ObjectMapper mapper = new ObjectMapper();

    JsonNode node = mapper.readTree("{\"a\": [{\"aa\":1}, {\"aa\": 2}] }");
    Document d = new Document("id1");
    d.setField("myField", node);

    assertEquals(d, Document.fromJsonString(d.toString()));
    assertEquals(d.toString(), Document.fromJsonString(d.toString()).toString());

    JsonNode node2 = mapper.readTree("{\"a\": [{\"aa\":1}, {\"aa\": 3}] }");
    Document d2 = new Document("id1");
    d2.setField("myField", node2);
    assertNotEquals(d, d2);

    d2.setField("myField", node.deepCopy());
    assertEquals(d, d2);
  }

  @Test
  public void testJsonObjectField() throws Exception {
    ObjectMapper mapper = new ObjectMapper();

    JsonNode node = mapper.readTree("{\"a\": {\"aa\":1}, \"b\":{\"ab\": 2} }");
    Document d = new Document("id1");
    d.setField("myField", node);

    assertEquals(d, Document.fromJsonString(d.toString()));
    assertEquals(d.toString(), Document.fromJsonString(d.toString()).toString());

    JsonNode node2 = mapper.readTree("{\"a\": {\"aa\":1}, \"b\":{\"ab\": 3} }");
    Document d2 = new Document("id1");
    d2.setField("myField", node2);
    assertNotEquals(d, d2);

    d2.setField("myField", node.deepCopy());
    assertEquals(d, d2);
  }

  @Test
  public void testGetAllFieldNames() {
    Document d = new Document("id");
    d.setField("field1", 1);
    d.setField("field2", 1);
    d.setField("field3", 16);

    Set<String> fieldNames = d.getFieldNames();
    // expect 4 fields : id, field1, field2, and field3
    assertEquals(4, fieldNames.size());
    assertTrue(fieldNames.contains("id"));
    assertTrue(fieldNames.contains("field1"));
    assertTrue(fieldNames.contains("field2"));
    assertTrue(fieldNames.contains("field3"));
  }

  @Test
  public void testRemoveDuplicateValuesWithNullTarget() {
    Document d = new Document("id");
    d.setField("field1", 1);
    d.addToField("field1", 1);
    d.addToField("field1", 16);
    d.addToField("field1", 129);

    d.removeDuplicateValues("field1", null);

    List<String> values1 = d.getStringList("field1");
    assertEquals("1", values1.get(0));
    assertEquals("16", values1.get(1));
    assertEquals("129", values1.get(2));

    // ensure that the numbers do not come out as Strings
    assertEquals("{\"id\":\"id\",\"field1\":[1,16,129]}", d.toString());

    d.setField("field2", "a");
    d.addToField("field2", "b");
    d.addToField("field2", "c");
    d.addToField("field2", "b");

    d.removeDuplicateValues("field2", null);

    List<String> values2 = d.getStringList("field2");
    assertEquals("a", values2.get(0));
    assertEquals("b", values2.get(1));
    assertEquals("c", values2.get(2));

    // ensure that the Strings do come out as Strings
    assertEquals(
        "{\"id\":\"id\",\"field1\":[1,16,129],\"field2\":[\"a\",\"b\",\"c\"]}", d.toString());
  }

  @Test
  public void testRemoveDuplicateValuesWithValidTarget() {
    Document d = new Document("id");
    d.setField("field1", 1);
    d.addToField("field1", 1);
    d.addToField("field1", 16);
    d.addToField("field1", 129);

    d.removeDuplicateValues("field1", "output");

    List<String> values1 = d.getStringList("output");
    assertEquals("1", values1.get(0));
    assertEquals("16", values1.get(1));
    assertEquals("129", values1.get(2));

    // verify that the original field stays the same, while the output field contains the correct
    // values
    assertEquals("{\"id\":\"id\",\"field1\":[1,1,16,129],\"output\":[1,16,129]}", d.toString());

    Document d2 = new Document("id2");
    d2.setField("field2", "a");
    d2.addToField("field2", "b");
    d2.addToField("field2", "c");
    d2.addToField("field2", "b");

    d2.removeDuplicateValues("field2", "field2");

    List<String> values2 = d2.getStringList("field2");
    assertEquals("a", values2.get(0));
    assertEquals("b", values2.get(1));
    assertEquals("c", values2.get(2));

    // verify in-place modification
    assertEquals("{\"id\":\"id2\",\"field2\":[\"a\",\"b\",\"c\"]}", d2.toString());
  }

  @Test
  public void testRemoveDuplicatesSameField() throws DocumentException, JsonProcessingException {
    Document d =
        Document.fromJsonString("{\"id\":\"123\", \"field1\":\"val1\", \"field2\":[\"1\"]}");

    // single valued
    d.removeDuplicateValues("field1", null);
    assertEquals(List.of("val1"), d.getStringList("field1"));

    // set with no duplicates
    d.removeDuplicateValues("field2", null);
    assertEquals(List.of(1), d.getIntList("field2"));
  }

  @Test
  public void testLogError() {
    Document d = new Document("id");
    assertFalse(d.has(Document.ERROR_FIELD));

    d.logError("error1");
    assertTrue(d.has(Document.ERROR_FIELD));
    assertEquals("error1", d.getString(Document.ERROR_FIELD));

    d.logError("error2");
    assertEquals(List.of("error1", "error2"), d.getStringList(Document.ERROR_FIELD));
  }

  @Test
  public void testGetChildrenException() throws DocumentException {

    // demonstrates the consequences of not copying the object given to the constructor

    // here the child is changed after being added to the parent and throws an error (that is
    // logged) when trying to retrieve children

    Document parent = new Document("id");

    ObjectNode child1Node = JsonNodeFactory.instance.objectNode();
    child1Node.put("id", "child1");
    Document child1 = new Document(child1Node);
    parent.addChild(child1);

    Document child2 = new Document("child2");
    parent.addChild(child2);

    // both children are present
    assertEquals(List.of(child1, child2), parent.getChildren());

    // after removing the id from child1 only child2 is returned when getting children
    child1Node.remove("id");
    assertEquals(List.of(child2), parent.getChildren());

    // did not find an easy way to check if error message has been logged
  }

  @Test(expected = IllegalStateException.class)
  public void testClone() throws DocumentException {

    // demonstrates the consequences of not copying the object given to the constructor

    // here the node used to create the document is changed after document construction and throws
    // an error when trying to clone the document

    ObjectNode node = JsonNodeFactory.instance.objectNode();
    node.put("id", "123");

    Document document = new Document(node);
    node.remove("id");
    document.clone();
  }
}
