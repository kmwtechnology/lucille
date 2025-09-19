package com.kmwllc.lucille.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.core.Document.Segment;
import java.sql.Timestamp;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.time.Instant;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.UnaryOperator;

import static com.kmwllc.lucille.core.Document.RESERVED_FIELDS;
import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public abstract class DocumentTest {

  public abstract Document createDocument(ObjectNode node) throws DocumentException;

  public abstract Document createDocument(String id);

  public abstract Document createDocument(String id, String runId);

  public Document createDocumentFromJson(String json) throws DocumentException, JsonProcessingException {
    return createDocumentFromJson(json, null);
  }

  public abstract Document createDocumentFromJson(String json, UnaryOperator<String> idUpdater)
      throws DocumentException, JsonProcessingException;

  @Test(expected = NullPointerException.class)
  public void testCreateWithoutId1() {
    createDocument((String) null);
  }

  @Test(expected = DocumentException.class)
  public void testCreateWithoutId2() throws Exception {
    createDocumentFromJson("{\"field1\":\"val1\"}");
  }

  @Test(expected = DocumentException.class)
  public void testCreateWithoutId3() throws Exception {
    createDocumentFromJson("{\"id\":\"\"}");
  }

  @Test(expected = DocumentException.class)
  public void testIntIdJson() throws Exception {
    createDocumentFromJson("{\"id\":1}");
  }

  @Test(expected = DocumentException.class)
  public void testNullIdJson() throws Exception {
    createDocumentFromJson("{\"id\":null}");
  }

  @Test
  public void testEqualsAndHashcode() throws Exception {

    Document doc1 = createDocumentFromJson("{\"id\":\"123\", \"field1\":\"val1\", \"field2\":\"val2\"}");
    Document doc2 = createDocumentFromJson("{\"id\":\"123\", \"field2\":\"val2\", \"field1\":\"val1\" }");
    Document doc3 = createDocumentFromJson("{\"id\":\"123\", \"field1\":\"val1\", \"field2\":\"val3\"}");

    assertEquals(doc1, doc1);
    assertEquals(doc1, doc2);
    assertEquals(doc2, doc1);
    assertNotEquals(doc3, doc1);
    assertNotEquals(doc1, doc3);
    assertNotEquals(doc1, new Object());
    assertEquals(doc1.hashCode(), doc2.hashCode());

    assertEquals(doc1.hashCode(), doc1.deepCopy().hashCode());
    assertEquals(doc1.deepCopy(), doc1);
    assertEquals(doc1, doc1.deepCopy());

    // hashcodes of unequal objects are not required to be unequal, but if these turned out to be
    // equal
    // it would be a cause for concern
    assertNotEquals(doc1.hashCode(), doc3.hashCode());
  }

  @Test
  public void testLength() {
    Document d = createDocument("id");
    d.setField("field1", 1);
    d.setField("field2", 1);
    d.addToField("field2", 2);
    d.addToField("field2", 3);
    assertEquals(1, d.length("field1"));
    assertEquals(3, d.length("field2"));
    assertEquals(0, d.length("field3"));
  }

  @Test
  public void testIsMultiValued() throws DocumentException, JsonProcessingException {
    Document document = createDocumentFromJson("" + "{\"id\":\"123\", " + "\"null\":null," + "\"single\":\"val1\", "
        + "\"empty_arr\":[]," + "\"arr1\":[\"val1\"]," + "\"arr2\":[\"val1\", \"val2\"]}");

    assertFalse(document.isMultiValued("id"));
    assertFalse(document.isMultiValued("null"));
    assertFalse(document.isMultiValued("single"));
    assertTrue(document.isMultiValued("empty_arr"));
    assertTrue(document.isMultiValued("arr1"));
    assertTrue(document.isMultiValued("arr2"));

    // not present field
    assertFalse(document.isMultiValued("not_present"));
  }

  @Test(expected = DocumentException.class)
  public void testCreateWithoutId4() throws Exception {
    createDocumentFromJson("{\"id\":null}");
  }

  @Test(expected = DocumentException.class)
  public void testCreateWithoutId5() throws Exception {
    createDocumentFromJson("{\"id\":1}");
  }

  @Test(expected = DocumentException.class)
  public void testCreateWithoutId6() throws Exception {
    createDocument(new ObjectMapper().createObjectNode());
  }

  @Test
  public void testCreateFromJsonString() throws Exception {
    // {"id":"123", "field1":"val1", "field2":"val2"}
    Document document = createDocumentFromJson("{\"id\":\"123\", \"field1\":\"val1\", \"field2\":\"val2\"}");
    assertEquals("123", document.getString("id"));
    assertEquals("123", document.getId());
    assertEquals("val1", document.getString("field1"));
    assertEquals("val2", document.getString("field2"));
  }

  @Test
  public void testCreateFromJsonStringWithUpdater() throws Exception {
    // {"id":"123", "field1":"val1", "field2":"val2"}
    UnaryOperator<String> updater = s -> "id_" + s;
    Document document = createDocumentFromJson("{\"id\":\"123\", \"field1\":\"val1\", \"field2\":\"val2\"}", updater);
    assertEquals("id_123", document.getString("id"));
    assertEquals("id_123", document.getId());
    assertEquals("val1", document.getString("field1"));
    assertEquals("val2", document.getString("field2"));
  }

  @Test
  public void testCreateFromID() {
    Document document = createDocument("123");
    assertEquals("123", document.getString("id"));
    assertEquals("123", document.getId());
  }

  @Test
  public void testCreateFromIdAndRunId() {
    Document document = createDocument("123", "456");
    assertEquals("123", document.getString("id"));
    assertEquals("123", document.getId());
    assertEquals("456", document.getString("run_id"));
    assertEquals("456", document.getRunId());
  }

  @Test
  public void testRemoveReservedField() {
    Document document = createDocument("123");
    for (String field : RESERVED_FIELDS) {
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
    Document document = createDocumentFromJson("{\"id\":\"123\", \"field1\":\"val1\", \"field2\":\"val2\"}");
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
    Document document = createDocumentFromJson("{\"id\":\"123\", \"array\":[\"val1\", \"val2\"]}");
    assertEquals(List.of("val1", "val2"), document.getStringList("array"));

    document.removeFromArray("array", 1);
    assertEquals(List.of("val1"), document.getStringList("array"));

    document.removeFromArray("array", 0);
    assertEquals(List.of(), document.getStringList("array"));
  }

  @Test
  public void testClearRunId() {
    Document document = createDocument("123", "456");
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
    Document document = createDocumentFromJson("{\"id\":\"123\", \"field1\":\"val1\", \"field2\":null}");
    assertTrue(document.hasNonNull("id"));
    assertTrue(document.hasNonNull("field1"));
    assertFalse(document.hasNonNull("field2"));
  }

  @Test
  public void testSetAndGetField() {
    Document document = createDocument("123");
    assertFalse(document.has("field1"));
    document.setField("field1", "val1");
    assertTrue(document.has("field1"));
    assertEquals("val1", document.getString("field1"));
  }

  @Test
  public void testSetFieldObject() throws JsonProcessingException {
    Document document = createDocument("123");
    // check that all types supported will not throw error
    // Int
    document.setField("integerField", (Object) 1);
    // Boolean
    document.setField("booleanField", (Object) true);
    // Long
    document.setField("longField", (Object) 2L);
    // Float
    document.setField("floatField", (Object) 2F);
    // Double
    document.setField("doubleField", (Object) 2D);
    // Date
    document.setField("dateField", (Object) new Date(1L));
    // Timestamp
    document.setField("timestampField", (Object) new Timestamp(2L));
    // Instant
    document.setField("instantField", (Object) Instant.ofEpochSecond(1));
    // JsonNode
    ObjectMapper map = new ObjectMapper();
    document.setField("jsonNodeField", (Object) map.readTree("{\"a\":1, \"b\":2}"));
    // byteArray
    byte[] bytes = new byte[] {0x3c, 0x4c, 0x5c};
    document.setField("byteArrayField", (Object) bytes);
  }

  @Test
  public void testGetUnsetField() {
    Document document = createDocument("id");
    assertFalse(document.has("test_field"));
    assertNull(document.getString("test_field"));
    assertNull(document.getStringList("test_field"));
  }

  @Test
  public void testAsMapReturnsCopy() {
    // confirm that modifying the map returned by Document.asMap does not affect the original document
    // this assumption is important in various Indexers where a Document is converted to a Map and then
    // various "ignoreFields" are removed from that map before indexing. The list of "ignoreFields" might include
    // the id field. But we never want the id of the _original_ document to be removed. In particular, that ID
    // is needed later in the workflow for accounting.
    Document document = createDocument("123");
    document.setField("myField", "hello");
    Map docMap = document.asMap();
    docMap.remove(Document.ID_FIELD);
    docMap.remove("myField");
    docMap.put("myField2", "hello2");
    assertEquals("123", document.getId());
    assertEquals("hello", document.getString("myField"));
    assertFalse(document.has("myField2"));

    // Note that we do not currently test whether asMap returns a shallow or deep copy of the document data.
    // For example, modifying the contents of an array value inside the Map might or might not affect the contents
    // of that same array in the original document. Deep copying could be tested as follows but HashMapDocument would not pass:
    // byte[] myBytes = new byte[]{0x3c, 0x4c, 0x5c};
    // document.setField("myBytes", myBytes);
    // ((byte[])docMap.get("myBytes"))[0] = 0x5c;
    // assertEquals(0x5c, document.getBytes("myBytes")[0]);
  }

  @Test
  public void testAddToField() {
    Document document = createDocument("123");
    assertFalse(document.has("field1"));
    document.addToField("field1", "val1");
    document.addToField("field1", "val2");
    document.addToField("field1", "val3");
    List<String> expected = Arrays.asList("val1", "val2", "val3");
    assertEquals(expected, document.getStringList("field1"));
  }

  @Test
  public void testAddToFieldObject() throws Exception {
    Document document = createDocument("123");
    assertFalse(document.has("field1"));
    // String
    document.addToField("field1", (Object) "val1");
    document.addToField("field1", (Object) "val2");
    document.addToField("field1", (Object) "val3");
    List<String> expected = Arrays.asList("val1", "val2", "val3");
    assertEquals(expected, document.getStringList("field1"));
    // Testing that error will not be thrown for all supported types
    // Int
    document.addToField("integerField", (Object) 1);
    // Boolean
    document.addToField("booleanField", (Object) true);
    // Long
    document.addToField("longField", (Object) 2L);
    // Float
    document.addToField("floatField", (Object) 2F);
    // Double
    document.addToField("doubleField", (Object) 2D);
    // Date
    document.addToField("dateField", (Object) new Date(1L));
    // Timestamp
    document.addToField("timestampField", (Object) new Timestamp(2L));
    // Instant
    document.addToField("instantField", (Object) Instant.ofEpochSecond(1));
    // JsonNode
    ObjectMapper map = new ObjectMapper();
    document.addToField("jsonNodeField", (Object) map.readTree("{\"a\":1, \"b\":2}"));
    // byteArray
    byte[] bytes = new byte[] {0x3c, 0x4c, 0x5c};
    document.addToField("byteArrayField", (Object) bytes);
  }

  @Test
  public void testWriteToField() {
    Document document = createDocument("doc");
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
    Document document = createDocument("doc");
    document.setField("pets", "dog");
    assertFalse(document.isMultiValued("pets"));
    assertEquals("dog", document.getString("pets"));
    assertEquals(Collections.singletonList("dog"), document.getStringList("pets"));
  }

  @Test
  public void testGetStringsMultiValued() {
    Document document = createDocument("doc");
    document.setField("pets", "dog");
    assertFalse(document.isMultiValued("pets"));
    document.addToField("pets", "cat");
    assertTrue(document.isMultiValued("pets"));
    document.addToField("pets", "fish");
    assertEquals(Arrays.asList("dog", "cat", "fish"), document.getStringList("pets"));
  }

  @Test
  public void testGetStringMultivalued() {
    Document document = createDocument("doc");
    document.addToField("field1", "val1");
    document.addToField("field1", "val2");
    assertEquals("val1", document.getString("field1"));
  }

  @Test
  public void testGetNullField() throws DocumentException, JsonProcessingException {

    Document document = createDocumentFromJson("{\"id\":\"doc\", \"field1\":null, \"field2\":[null]}");

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
  }

  @Test
  public void testGetIntMissing() {
    Document document = createDocument("doc");
    assertNull(document.getInt("field1"));
  }

  @Test
  public void testGetIntSingleValued() {
    Document document = createDocument("doc");
    document.setField("number", 1);
    assertFalse(document.isMultiValued("number"));
    assertEquals(1, document.getInt("number").intValue());
    assertEquals(Collections.singletonList(1), document.getIntList("number"));
  }

  @Test
  public void testGetIntListMissing() {
    Document document = createDocument("doc");
    assertNull(document.getIntList("field1"));
  }

  @Test
  public void testGetIntsMultiValued() {
    Document document = createDocument("doc");
    document.setField("pets", 3);
    assertFalse(document.isMultiValued("pets"));
    document.addToField("pets", 2);
    assertTrue(document.isMultiValued("pets"));
    document.addToField("pets", 49);
    assertEquals(Arrays.asList(3, 2, 49), document.getIntList("pets"));
  }

  @Test
  public void testGetIntMultivalued() {
    Document document = createDocument("doc");
    document.addToField("field1", 16);
    document.addToField("field1", -38);
    assertEquals(16, document.getInt("field1").intValue());
  }

  @Test
  public void testGetDoubleMissing() {
    Document document = createDocument("doc");
    assertNull(document.getDouble("field1"));
  }

  @Test
  public void testGetDoubleSingleValued() {
    Document document = createDocument("doc");
    document.setField("double", 1.455);
    assertFalse(document.isMultiValued("double"));
    assertEquals(1.455, document.getDouble("double"), 0);
    assertEquals(Collections.singletonList(1.455), document.getDoubleList("double"));
  }

  @Test
  public void testGetDoubleListMissing() {
    Document document = createDocument("doc");
    assertNull(document.getDoubleList("field1"));
  }

  @Test
  public void testGetDoublesMultiValued() {
    Document document = createDocument("doc");
    document.setField("gpa", 4.0);
    assertFalse(document.isMultiValued("gpa"));
    document.addToField("gpa", 2);
    assertTrue(document.isMultiValued("gpa"));
    document.addToField("gpa", 2.3);
    assertEquals(Arrays.asList(4.0, 2.0, 2.3), document.getDoubleList("gpa"));
  }

  @Test
  public void testGetDoubleMultivalued() {
    Document document = createDocument("doc");
    document.addToField("field1", 16.44);
    document.addToField("field1", -38.91);
    assertEquals(16.44, document.getDouble("field1"), 0);
  }

  @Test
  public void testGetBooleanMissing() {
    Document document = createDocument("doc");
    assertNull(document.getBoolean("field1"));
  }

  @Test
  public void testGetBooleanSingleValued() {
    Document document = createDocument("doc");
    document.setField("bool", true);
    assertFalse(document.isMultiValued("bool"));
    assertEquals(true, document.getBoolean("bool"));
    assertEquals(Collections.singletonList(true), document.getBooleanList("bool"));
  }

  @Test
  public void testGetBooleanListMissing() {
    Document document = createDocument("doc");
    assertNull(document.getBooleanList("field1"));
  }

  @Test
  public void testGetBooleansMultiValued() {
    Document document = createDocument("doc");
    document.setField("bools", true);
    assertFalse(document.isMultiValued("bools"));
    document.addToField("bools", false);
    assertTrue(document.isMultiValued("bools"));
    document.addToField("bools", false);
    assertEquals(Arrays.asList(true, false, false), document.getBooleanList("bools"));
  }

  @Test
  public void testGetBooleanMultivalued() {
    Document document = createDocument("doc");
    document.addToField("field1", true);
    document.addToField("field1", false);
    assertEquals(true, document.getBoolean("field1"));
  }

  @Test
  public void testGetLongMissing() {
    Document document = createDocument("doc");
    assertNull(document.getLong("field1"));
  }

  @Test
  public void testGetLongSingleValued() {
    Document document = createDocument("doc");
    document.setField("long", 1000000L);
    assertFalse(document.isMultiValued("long"));
    assertEquals(1000000L, document.getLong("long").longValue());
    assertEquals(Collections.singletonList(1000000L), document.getLongList("long"));
  }

  @Test
  public void testGetLongListMissing() {
    Document document = createDocument("doc");
    assertNull(document.getLongList("field1"));
  }

  @Test
  public void testGetLongsMultiValued() {
    Document document = createDocument("doc");
    document.setField("longs", 14L);
    assertFalse(document.isMultiValued("longs"));
    document.addToField("longs", 1234L);
    assertTrue(document.isMultiValued("longs"));
    document.addToField("longs", -3L);
    assertEquals(Arrays.asList(14L, 1234L, -3L), document.getLongList("longs"));
  }

  @Test
  public void testGetLongMultivalued() {
    Document document = createDocument("doc");
    document.addToField("field1", 3L);
    document.addToField("field1", 1933384L);
    assertEquals(3L, document.getLong("field1").longValue());
  }

  @Test
  public void testGetFloatMissing() {
    Document document = createDocument("doc");
    assertNull(document.getFloat("field1"));
  }

  @Test
  public void testGetFloatSingleValued() {
    Document document = createDocument("doc");
    document.setField("float", 1.1F);
    assertFalse(document.isMultiValued("float"));
    assertEquals(1.1F, document.getFloat("float").floatValue(), 0);
    assertEquals(Collections.singletonList(1.1F), document.getFloatList("float"));
  }

  @Test
  public void testGetFloatListMissing() {
    Document document = createDocument("doc");
    assertNull(document.getFloatList("field1"));
  }

  @Test
  public void testGetFloatsMultiValued() {
    Document document = createDocument("doc");
    document.setField("floats", 1.1F);
    assertFalse(document.isMultiValued("floats"));
    document.addToField("floats", 2.2F);
    assertTrue(document.isMultiValued("floats"));
    document.addToField("floats", 3.3F);
    assertEquals(Arrays.asList(1.1F, 2.2F, 3.3F), document.getFloatList("floats"));
  }

  @Test
  public void testGetFloatMultivalued() {
    Document document = createDocument("doc");
    document.addToField("field1", 1.1F);
    document.addToField("field1", 2.2F);
    assertEquals(1.1F, document.getFloat("field1").floatValue(), 0);
  }

  @Test
  public void testGetInstantMissing() {
    Document document = createDocument("doc");
    assertNull(document.getInstant("field1"));
  }

  @Test
  public void testGetInstantSingleValued() {
    Document document = createDocument("doc");
    document.setField("instant", Instant.ofEpochSecond(10000));
    assertFalse(document.isMultiValued("instant"));
    assertEquals(Instant.ofEpochSecond(10000), document.getInstant("instant"));
    assertEquals(Collections.singletonList(Instant.ofEpochSecond(10000)), document.getInstantList("instant"));
    assertEquals("1970-01-01T02:46:40Z", document.getString("instant"));
  }

  @Test
  public void testGetInstantListMissing() {
    Document document = createDocument("doc");
    assertNull(document.getInstantList("field1"));
  }

  @Test
  public void testGetInstantsMultiValued() {
    Document document = createDocument("doc");
    document.setField("instants", Instant.ofEpochSecond(44));
    assertFalse(document.isMultiValued("instants"));
    document.addToField("instants", Instant.ofEpochSecond(1033000));
    assertTrue(document.isMultiValued("instants"));
    document.addToField("instants", Instant.ofEpochSecond(143242));
    assertEquals(Arrays.asList(Instant.ofEpochSecond(44), Instant.ofEpochSecond(1033000), Instant.ofEpochSecond(143242)),
        document.getInstantList("instants"));
  }

  @Test
  public void testGetInstantMultivalued() {
    Document document = createDocument("doc");
    document.addToField("field1", Instant.ofEpochSecond(44));
    document.addToField("field1", Instant.ofEpochSecond(94));
    assertEquals(Instant.ofEpochSecond(44), document.getInstant("field1"));
  }

  @Test
  public void testGetDateMissing() {
    Document document = createDocument("doc");
    assertNull(document.getDate("field1"));
  }

  @Test
  public void testGetDateSingleValued() {
    Document document = createDocument("doc");
    document.setField("date", new Date(1L));
    assertFalse(document.isMultiValued("date"));
    assertEquals(new Date(1L), document.getDate("date"));
    assertEquals(Collections.singletonList(new Date(1L)), document.getDateList("date"));
    // date is stored as an ISO INSTANT formatted string in JsonDocument, but stored as a date object in HashmapDocument
    // output of getString is thus dependent on type of document
  }

  @Test
  public void testGetDateListMissing() {
    Document document = createDocument("doc");
    assertNull(document.getDateList("field1"));
  }

  @Test
  public void testGetDatesMultiValued() {
    Document document = createDocument("doc");
    document.setField("dates", new Date(1L));
    assertFalse(document.isMultiValued("dates"));
    document.addToField("dates", new Date(2L));
    assertTrue(document.isMultiValued("dates"));
    document.addToField("dates", new Date(3L));
    assertEquals(Arrays.asList(new Date(1L), new Date(2L), new Date(3L)), document.getDateList("dates"));
  }

  @Test
  public void testGetDateMultivalued() {
    Document document = createDocument("doc");
    document.addToField("field1", new Date(1));
    document.addToField("field1", new Date(2));
    assertEquals(new Date(1), document.getDate("field1"));
  }

  @Test
  public void testGetTimestampMissing() {
    Document document = createDocument("doc");
    assertNull(document.getTimestamp("field1"));
  }

  @Test
  public void testGetTimestampSingleValued() {
    Document document = createDocument("doc");
    document.setField("timestamp", new Timestamp(1L));
    assertFalse(document.isMultiValued("timestamp"));
    assertEquals(new Timestamp(1L), document.getTimestamp("timestamp"));
    assertEquals(Collections.singletonList(new Timestamp(1L)), document.getTimestampList("timestamp"));
    // timestamp is stored as an ISO INSTANT formatted string in JsonDocument, but stored as a timestamp object in HashmapDocument
    // output of getString is thus dependent on type of document
  }

  @Test
  public void testGetTimestampListMissing() {
    Document document = createDocument("doc");
    assertNull(document.getTimestampList("field1"));
  }

  @Test
  public void testGetTimestampMultiValued() {
    Document document = createDocument("doc");
    document.setField("timestamp", new Timestamp(1L));
    assertFalse(document.isMultiValued("timestamp"));
    document.addToField("timestamp", new Timestamp(2L));
    assertTrue(document.isMultiValued("timestamp"));
    document.addToField("timestamp", new Timestamp(3L));
    assertEquals(Arrays.asList(new Timestamp(1L), new Timestamp(2L), new Timestamp(3L)), document.getTimestampList("timestamp"));
  }

  @Test
  public void testGetTimestampMultivalued() {
    Document document = createDocument("doc");
    document.addToField("field1", new Timestamp(1L));
    document.addToField("field1", new Timestamp(2L));
    assertEquals(new Timestamp(1L), document.getTimestamp("field1"));
  }

  @Test
  public void testChildren() throws Exception {
    Document parent = createDocument("parent");
    assertFalse(parent.hasChildren());

    Document child1 = createDocument("child1");
    child1.setField("field1", "val1");

    Document child2 = createDocument("child2");
    child2.setField("field1", "val1b");
    child1.setField("field2", "val2");

    parent.addChild(child1);
    assertTrue(parent.hasChildren());
    parent.addChild(child2);

    List<Document> children = parent.getChildren();
    assertEquals(2, children.size());
    assertEquals(child1, children.get(0));
    assertEquals(child2, children.get(1));

    Document deserializedParent = createDocumentFromJson(parent.toString());
    assertEquals(parent, deserializedParent);
    assertTrue(deserializedParent.hasChildren());

    List<Document> deserializedChildren = deserializedParent.getChildren();
    assertEquals(child1, deserializedChildren.get(0));
    assertEquals(child2, deserializedChildren.get(1));
  }

  @Test
  public void testEmptyChildren() throws Exception {
    Document parent = createDocument("parent");
    String beforeString = parent.toString();
    List<Document> children = parent.getChildren();
    String afterString = parent.toString();
    // getChildren() should not create a .children field if it didn't already exist,
    // so the json-stringified form of the document should not change after calling getChildren()
    assertEquals(beforeString, afterString);
    assertEquals(0, children.size());
    assertEquals(parent, createDocumentFromJson(parent.toString()));
  }

  @Test
  public void testNullHandling() throws Exception {
    // set a field to null and confirm that we get back a null when we call getString(), not the
    // string "null"
    Document document = createDocument("doc");
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
    document = createDocumentFromJson(document.toString());
    field1 = document.getStringList("field1");
    assertNull(field1.get(0));
    assertNull(field1.get(1));
    assertEquals(2, field1.size());
    assertTrue(document.isMultiValued("field1"));
  }

  @Test
  public void testRenameField() {
    Document document = createDocument("doc");
    document.addToField("initial", "first");
    document.addToField("initial", "second");
    document.renameField("initial", "final", UpdateMode.SKIP);
    List<String> values = document.getStringList("final");
    assertFalse(document.has("initial"));
    assertEquals(List.of("first", "second"), values);
  }

  @Test
  public void testRenameOverwrite() {
    Document document = createDocument("document");
    document.setField("initial", "first");
    document.setField("final", "will be repalced");
    assertTrue(document.has("final"));
    document.renameField("initial", "final", UpdateMode.OVERWRITE);
    assertEquals("first", document.getString("final"));
  }

  @Test
  public void testRenameAppend() {
    Document document = createDocument("document");
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
    Document document = createDocument("doc");
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
    Document document = createDocumentFromJson("{\"id\":\"123\", \"field1\":\"val1\"}");
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
    Document document = createDocument("document");
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
    Document document = createDocument("id1");
    document.update("myStringField", UpdateMode.OVERWRITE, "val1");
    document.update("myStringField", UpdateMode.OVERWRITE, "val2");
    document.update("myStringField", UpdateMode.APPEND, "val3");
    document.update("myStringField", UpdateMode.SKIP, "val4");
    assertEquals(List.of("val2", "val3"), valueListFromDocument(document, "myStringField"));
  }

  @Test
  public void testUpdateObjectString() throws Exception {
    Document document = createDocument("id1");
    document.update("myStringField", UpdateMode.OVERWRITE, (Object) "val1");
    document.update("myStringField", UpdateMode.OVERWRITE, (Object) "val2");
    document.update("myStringField", UpdateMode.APPEND, (Object) "val3");
    document.update("myStringField", UpdateMode.SKIP, (Object) "val4");
    assertEquals(List.of("val2", "val3"), valueListFromDocument(document, "myStringField"));
  }

  @Test
  public void testUpdateInt() {
    Document document = createDocument("id1");
    document.update("myIntField", UpdateMode.OVERWRITE, 1);
    document.update("myIntField", UpdateMode.OVERWRITE, 2);
    document.update("myIntField", UpdateMode.APPEND, 3);
    document.update("myIntField", UpdateMode.SKIP, 4);
    assertEquals(List.of(2, 3), valueListFromDocument(document, "myIntField"));
  }


  @Test
  public void testUpdateIntObject() throws Exception {
    Document document = createDocument("id1");
    document.update("myIntField", UpdateMode.OVERWRITE, (Object) 1);
    document.update("myIntField", UpdateMode.OVERWRITE, (Object) 2);
    document.update("myIntField", UpdateMode.APPEND, (Object) 3);
    document.update("myIntField", UpdateMode.SKIP, (Object) 4);
    assertEquals(List.of(2, 3), valueListFromDocument(document, "myIntField"));
  }

  @Test
  public void testUpdateLong() {
    Document document = createDocument("id1");
    document.update("myLongField", UpdateMode.OVERWRITE, 1L);
    document.update("myLongField", UpdateMode.OVERWRITE, 2L);
    document.update("myLongField", UpdateMode.APPEND, 3L);
    document.update("myLongField", UpdateMode.SKIP, 4L);
    assertEquals(List.of(2L, 3L), valueListFromDocument(document, "myLongField"));
  }

  @Test
  public void testUpdateLongObject() throws Exception {
    Document document = createDocument("id1");
    document.update("myLongField", UpdateMode.OVERWRITE, (Object) 1L);
    document.update("myLongField", UpdateMode.OVERWRITE, (Object) 2L);
    document.update("myLongField", UpdateMode.APPEND, (Object) 3L);
    document.update("myLongField", UpdateMode.SKIP, (Object) 4L);
    assertEquals(List.of(2L, 3L), valueListFromDocument(document, "myLongField"));
  }

  @Test
  public void testUpdateDouble() {
    Document document = createDocument("id1");
    document.update("myDoubleField", UpdateMode.OVERWRITE, 1D);
    document.update("myDoubleField", UpdateMode.OVERWRITE, 2D);
    document.update("myDoubleField", UpdateMode.APPEND, 3D);
    document.update("myDoubleField", UpdateMode.SKIP, 4D);
    assertEquals(List.of(2D, 3D), valueListFromDocument(document, "myDoubleField"));
  }


  @Test
  public void testUpdateDoubleObject() throws Exception {
    Document document = createDocument("id1");
    document.update("myDoubleField", UpdateMode.OVERWRITE, (Object) 1D);
    document.update("myDoubleField", UpdateMode.OVERWRITE, (Object) 2D);
    document.update("myDoubleField", UpdateMode.APPEND, (Object) 3D);
    document.update("myDoubleField", UpdateMode.SKIP, (Object) 4D);
    assertEquals(List.of(2D, 3D), valueListFromDocument(document, "myDoubleField"));
  }

  @Test
  public void testUpdateBoolean() {
    Document document = createDocument("id1");
    document.update("myBooleanField", UpdateMode.OVERWRITE, true);
    document.update("myBooleanField", UpdateMode.OVERWRITE, false);
    document.update("myBooleanField", UpdateMode.APPEND, true);
    document.update("myBooleanField", UpdateMode.SKIP, false);
    assertEquals(List.of(false, true), valueListFromDocument(document, "myBooleanField"));
  }

  @Test
  public void testUpdateBooleanObject() throws Exception {
    Document document = createDocument("id1");
    document.update("myBooleanField", UpdateMode.OVERWRITE, (Object) true);
    document.update("myBooleanField", UpdateMode.OVERWRITE, (Object) false);
    document.update("myBooleanField", UpdateMode.APPEND, (Object) true);
    document.update("myBooleanField", UpdateMode.SKIP, (Object) false);
    assertEquals(List.of(false, true), valueListFromDocument(document, "myBooleanField"));
  }

  @Test
  public void testUpdateInstant() {

    Document document = createDocument("id1");
    document.update("myInstantField", UpdateMode.OVERWRITE, Instant.ofEpochSecond(1));
    document.update("myInstantField", UpdateMode.OVERWRITE, Instant.ofEpochSecond(2));
    document.update("myInstantField", UpdateMode.APPEND, Instant.ofEpochSecond(3));
    document.update("myInstantField", UpdateMode.SKIP, Instant.ofEpochSecond(4));

    assertEquals(List.of(Instant.ofEpochSecond(2), Instant.ofEpochSecond(3)), document.getInstantList("myInstantField"));
  }

  @Test
  public void testUpdateInstantObject() throws Exception {

    Document document = createDocument("id1");
    document.update("myInstantField", UpdateMode.OVERWRITE, (Object) Instant.ofEpochSecond(1));
    document.update("myInstantField", UpdateMode.OVERWRITE, (Object) Instant.ofEpochSecond(2));
    document.update("myInstantField", UpdateMode.APPEND, (Object) Instant.ofEpochSecond(3));
    document.update("myInstantField", UpdateMode.SKIP, (Object) Instant.ofEpochSecond(4));

    assertEquals(List.of(Instant.ofEpochSecond(2), Instant.ofEpochSecond(3)), document.getInstantList("myInstantField"));
  }

  @Test
  public void testUpdateDate() throws Exception {
    Document document = createDocument("id1");
    document.update("myDateField", UpdateMode.OVERWRITE, new Date(1));
    document.update("myDateField", UpdateMode.OVERWRITE, new Date(2));
    document.update("myDateField", UpdateMode.APPEND, new Date(3));
    document.update("myDateField", UpdateMode.SKIP, new Date(4));

    assertEquals(List.of(new Date(2), new Date(3)), document.getDateList("myDateField"));
  }

  @Test
  public void testUpdateDateObject() throws Exception {
    Document document = createDocument("id1");
    document.update("myDateField", UpdateMode.OVERWRITE, (Object) new Date(1));
    document.update("myDateField", UpdateMode.OVERWRITE, (Object) new Date(2));
    document.update("myDateField", UpdateMode.APPEND, (Object) new Date(3));
    document.update("myDateField", UpdateMode.SKIP, (Object) new Date(4));

    assertEquals(List.of(new Date(2), new Date(3)), document.getDateList("myDateField"));
  }

  @Test
  public void testUpdateTimestamp() throws Exception {
    Document document = createDocument("id1");
    document.update("myTimestampField", UpdateMode.OVERWRITE, new Timestamp(1));
    document.update("myTimestampField", UpdateMode.OVERWRITE, new Timestamp(2));
    document.update("myTimestampField", UpdateMode.APPEND, new Timestamp(3));
    document.update("myTimestampField", UpdateMode.SKIP, new Timestamp(4));

    assertEquals(List.of(new Timestamp(2), new Timestamp(3)), document.getTimestampList("myTimestampField"));
  }

  @Test
  public void testUpdateTimestampObject() throws Exception {
    Document document = createDocument("id1");
    document.update("myTimestampField", UpdateMode.OVERWRITE, (Object) new Timestamp(1));
    document.update("myTimestampField", UpdateMode.OVERWRITE, (Object) new Timestamp(2));
    document.update("myTimestampField", UpdateMode.APPEND, (Object) new Timestamp(3));
    document.update("myTimestampField", UpdateMode.SKIP, (Object) new Timestamp(4));

    assertEquals(List.of(new Timestamp(2), new Timestamp(3)), document.getTimestampList("myTimestampField"));
  }

  @Test
  public void testUpdateSingleVersusMultiValued() {
    Document document = createDocument("id1");
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

  /**
   * Demonstrates that update method throws an IllegalArgumentException when one of the provided values
   * is of an unsupported type (in this case, a List). In this case, updates that were made prior to
   * encountering the illegal value will persist and will not be reverted. Type checking is done
   * as each value is encountered and not in a single-pass at the beginning.
   *
   * @throws Exception
   */
  @Test
  public void testUpdateFailsInMiddle() throws Exception {
    Document document = createDocument("id1");
    Exception e = assertThrows(IllegalArgumentException.class, () -> {
      document.update("myField", UpdateMode.OVERWRITE, (Object) 5, (Object) 6, List.of(1), (Object) 7);
    });
    assertEquals("Type " + List.of(1).getClass().getName() + " is not supported", e.getMessage());
    assertEquals(2, document.getFieldNames().size());
    assertEquals(List.of(5, 6), document.getIntList("myField"));
  }

  @Test
  public void testUpdateUnsupportedType() {
    Document document = createDocument("id1");
    assertThrows(Exception.class, () -> {
      document.update("myField", UpdateMode.OVERWRITE, List.of(1));
    });

    assertEquals(1, document.getFieldNames().size());
  }

  @Test(expected = Exception.class)
  public void testSetDocIdFails() {
    Document document = createDocument("id1");
    document.setField(Document.ID_FIELD, "id2");
  }

  @Test(expected = Exception.class)
  public void testAddToDocIdFails() {
    Document document = createDocument("id1");
    document.addToField(Document.ID_FIELD, "id2");
  }

  @Test(expected = Exception.class)
  public void testUpdateDocIdFails() {
    Document document = createDocument("id1");
    document.update(Document.ID_FIELD, UpdateMode.OVERWRITE, "id2");
  }

  @Test(expected = Exception.class)
  public void testSetRunIdFails() {
    Document document = createDocument("id1");
    document.initializeRunId("run_id1");
    document.setField(Document.RUNID_FIELD, "id2");
  }

  @Test(expected = Exception.class)
  public void testReInitializeRunIdFails() {
    Document document = createDocument("id1");
    document.initializeRunId("run_id1");
    document.initializeRunId("run_id2");
  }

  @Test
  public void testSetOrAdd() {

    // confirm setOrAdd behaves as expected
    Document document = createDocument("id1");
    document.setOrAdd("field1", "value1");
    assertFalse(document.isMultiValued("field1"));
    assertEquals("value1", document.getString("field1"));
    document.setOrAdd("field1", "value2");
    assertTrue(document.isMultiValued("field1"));
    assertEquals("value1", document.getStringList("field1").get(0));
    assertEquals("value2", document.getStringList("field1").get(1));
    assertEquals(2, document.getStringList("field1").size());

    // compare with setField behavior
    Document document2 = createDocument("id2");
    document2.setField("field1", "value1");
    assertFalse(document2.isMultiValued("field1"));
    assertEquals("value1", document2.getString("field1"));
    document2.setField("field1", "value2");
    assertFalse(document2.isMultiValued("field1"));
    assertEquals("value2", document2.getString("field1"));

    // compare with addToField behavior
    Document document3 = createDocument("id1");
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
  public void testSetOrAddObject() throws Exception {

    // confirm setOrAdd behaves as expected
    Document document = createDocument("id1");
    document.setOrAdd("field1", (Object) "value1");
    assertFalse(document.isMultiValued("field1"));
    assertEquals("value1", document.getString("field1"));
    document.setOrAdd("field1", (Object) "value2");
    assertTrue(document.isMultiValued("field1"));
    assertEquals("value1", document.getStringList("field1").get(0));
    assertEquals("value2", document.getStringList("field1").get(1));
    assertEquals(2, document.getStringList("field1").size());

    // compare with setField behavior
    Document document2 = createDocument("id2");
    document2.setField("field1", (Object) "value1");
    assertFalse(document2.isMultiValued("field1"));
    assertEquals("value1", document2.getString("field1"));
    document2.setField("field1", (Object) "value2");
    assertFalse(document2.isMultiValued("field1"));
    assertEquals("value2", document2.getString("field1"));

    // compare with addToField behavior
    Document document3 = createDocument("id1");
    document3.addToField("field1", (Object) "value1");
    assertTrue(document3.isMultiValued("field1"));
    assertEquals("value1", document3.getString("field1"));
    document3.addToField("field1", (Object) "value2");
    assertTrue(document3.isMultiValued("field1"));
    assertEquals("value1", document3.getStringList("field1").get(0));
    assertEquals("value2", document3.getStringList("field1").get(1));
    assertEquals(2, document3.getStringList("field1").size());

    // test setOrAdd supporting all types as Objects
    // Int
    document.setOrAdd("integerField", (Object) 1);
    // Boolean
    document.setOrAdd("booleanField", (Object) true);
    // Long
    document.setOrAdd("longField", (Object) 2L);
    // Float
    document.setOrAdd("floatField", (Object) 2F);
    // Double
    document.setOrAdd("doubleField", (Object) 2D);
    // Date
    document.setOrAdd("dateField", (Object) new Date(1L));
    // Timestamp
    document.setOrAdd("timestampField", (Object) new Timestamp(2L));
    // Instant
    document.setOrAdd("instantField", (Object) Instant.ofEpochSecond(1));
    // JsonNode
    ObjectMapper map = new ObjectMapper();
    document.setOrAdd("jsonNodeField", (Object) map.readTree("{\"a\":1, \"b\":2}"));
    // byteArray
    byte[] bytes = new byte[] {0x3c, 0x4c, 0x5c};
    document.setOrAdd("byteArrayField", (Object) bytes);
  }

  @Test
  public void testSetOrAddInstant() {
    Document document = createDocument("id1");
    assertFalse(document.has("instant"));

    document.setOrAdd("instant", Instant.ofEpochSecond(10000));
    assertTrue(document.has("instant"));
    assertFalse(document.isMultiValued("instant"));

    document.setOrAdd("instant", Instant.ofEpochSecond(20000));
    assertTrue(document.isMultiValued("instant"));
  }

  @Test
  public void testSetOrAddInstantObject() throws Exception {
    Document document = createDocument("id1");
    assertFalse(document.has("instant"));

    document.setOrAdd("instant", (Object) Instant.ofEpochSecond(10000));
    assertTrue(document.has("instant"));
    assertFalse(document.isMultiValued("instant"));

    document.setOrAdd("instant", (Object) Instant.ofEpochSecond(20000));
    assertTrue(document.isMultiValued("instant"));
  }

  @Test
  public void testSetOrAddDocumentSingleValued() {

    Document document = createDocument("id1");
    Document otherDoc = createDocument("id2");

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

    Document document = createDocument("id1");
    Document otherDoc = createDocument("id2");

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

    Document d1 = createDocument("id1");
    d1.initializeRunId("run1");
    d1.setField("stringField", "val");
    d1.setField("intField", 1);
    d1.setField("boolField", true);
    d1.setField("doubleField", 2.0);
    d1.setField("longField", 3L);

    Document d2 = d1.deepCopy();
    d1.setOrAddAll(d2);
    d1.setOrAddAll(d2);
    d1.setOrAddAll(d2);

    Document expected = createDocument("id1");
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
    Document d = createDocument("id1");
    d.setField("myField", node);
    assertEquals(node, d.getJson("myField"));
    //assertFalse(d.isMultiValued("myField"));

    assertEquals(d, createDocumentFromJson(d.toString()));
    Document reconstructed = createDocumentFromJson(d.toString());
    assertEquals(d.toString(), reconstructed.toString());
    assertEquals(node, reconstructed.getJson("myField"));

    assertEquals(node, reconstructed.getJson("myField"));

    JsonNode node2 = mapper.readTree("{\"a\":1, \"b\":3}");
    Document d2 = createDocument("id1");
    d2.setField("myField", node2);
    assertNotEquals(d, d2);

    d2.setField("myField", node.deepCopy());
    assertEquals(d, d2);
  }

  @Test
  public void testJsonObjectField() throws Exception {
    ObjectMapper mapper = new ObjectMapper();

    JsonNode node = mapper.readTree("{\"a\": {\"aa\":1}, \"b\":{\"ab\": 2} }");
    Document d = createDocument("id1");
    d.setField("myField", node);

    assertEquals(d, createDocumentFromJson(d.toString()));
    assertEquals(d.toString(), createDocumentFromJson(d.toString()).toString());

    JsonNode node2 = mapper.readTree("{\"a\": {\"aa\":1}, \"b\":{\"ab\": 3} }");
    Document d2 = createDocument("id1");
    d2.setField("myField", node2);
    assertNotEquals(d, d2);

    d2.setField("myField", node.deepCopy());
    assertEquals(d, d2);
  }

  @Test
  public void testGetAllFieldNames() {
    Document d = createDocument("id");
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
    Document d = createDocument("id");
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
    // compareToString("{\"id\":\"id\",\"field1\":[1,16,129]}", d.toString());

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
    assertEquals("{\"id\":\"id\",\"field1\":[1,16,129],\"field2\":[\"a\",\"b\",\"c\"]}", d.toString());
    // compareToString("{\"id\":\"id\",\"field1\":[1,16,129],\"field2\":[\"a\",\"b\",\"c\"]}", d.toString());
  }

  private void assertEqualsFromString(Document document, String string) {
    try {
      Document created = createDocumentFromJson(string);
      assertEquals(document, created);
    } catch (DocumentException | JsonProcessingException e) {
      fail();
    }
  }

  @Test
  public void testRemoveDuplicateValuesWithValidTarget() {
    Document d = createDocument("id");
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
    // todo does the order matter here?
    // assertEquals("{\"id\":\"id\",\"field1\":[1,1,16,129],\"output\":[1,16,129]}", d.toString());
    assertEqualsFromString(d, "{\"id\":\"id\",\"field1\":[1,1,16,129],\"output\":[1,16,129]}");

    Document d2 = createDocument("id2");
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
    // compareToString("{\"id\":\"id2\",\"field2\":[\"a\",\"b\",\"c\"]}", d2.toString());
  }

  @Test
  public void testRemoveDuplicatesSameField() throws DocumentException, JsonProcessingException {
    Document d = createDocumentFromJson("{\"id\":\"123\", \"field1\":\"val1\", \"field2\":[\"1\"]}");

    // single valued
    d.removeDuplicateValues("field1", null);
    assertEquals(List.of("val1"), d.getStringList("field1"));

    // set with no duplicates
    d.removeDuplicateValues("field2", null);
    assertEquals(List.of(1), d.getIntList("field2"));
  }

  // todo use if decide to use HashMap instead of LinkedHashMap
  private void compareToString(String a, String b) {
    assertEquals(fromString(a), fromString(b));
  }

  private JsonNode fromString(String json) {
    try {
      return new ObjectMapper().readTree(json);
    } catch (JsonProcessingException e) {
      fail();
    }
    throw new RuntimeException("should not get here");
  }

  abstract static class NodeDocumentTest extends DocumentTest {

    @Test
    public void testGetNullFieldExceptions() throws DocumentException, JsonProcessingException {

      Document document = createDocumentFromJson("{\"id\":\"doc\", \"field1\":null, \"field2\":[null]}");

      List<Object> nullList = new ArrayList<>();
      nullList.add(null);

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
    public void testGetChildrenException() throws DocumentException {

      // demonstrates the consequences of not copying the object given to the constructor

      // here the child is changed after being added to the parent and throws an error (that is
      // logged) when trying to retrieve children

      Document parent = createDocument("id");

      ObjectNode child1Node = JsonNodeFactory.instance.objectNode();
      child1Node.put("id", "child1");
      Document child1 = createDocument(child1Node);
      parent.addChild(child1);

      Document child2 = createDocument("child2");
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

      Document document = createDocument(node);
      node.remove("id");
      document.deepCopy();
    }
  }

  @Test
  public void testGetBytesMissing() {
    Document document = createDocument("doc");
    assertNull(document.getBytes("field1"));
  }

  @Test
  public void testGetBytesSingleValued() {
    byte[] value = new byte[] {0x3c, 0x4c, 0x5c};
    Document document = createDocument("doc");
    document.setField("bytes", value);
    assertFalse(document.isMultiValued("bytes"));
    assertTrue(value.equals(document.getBytes("bytes")));
    assertEquals(value, document.getBytes("bytes"));
    assertEquals(Collections.singletonList(value), document.getBytesList("bytes"));
  }

  @Test
  public void testGetBytesListMissing() {
    Document document = createDocument("doc");
    assertNull(document.getBytesList("field1"));
  }

  @Test
  public void testGetBytesListMultiValued() throws Exception {
    byte[] value1 = new byte[] {0x3c, 0x4c, 0x5c};
    byte[] value2 = new byte[] {0x4c, 0x4c, 0x5c};
    byte[] value3 = new byte[] {0x5c, 0x4c, 0x5c};
    Document document = createDocument("doc");
    document.setField("bytes", value1);
    assertFalse(document.isMultiValued("bytes"));
    document.addToField("bytes", value2);
    assertTrue(document.isMultiValued("bytes"));
    document.addToField("bytes", value3);
    assertEquals(Arrays.asList(value1, value2, value3), document.getBytesList("bytes"));
  }

  @Test
  public void testGetBytesMultivalued() {
    byte[] value1 = new byte[] {0x3c, 0x4c, 0x5c};
    byte[] value2 = new byte[] {0x4c, 0x4c, 0x5c};
    Document document = createDocument("doc");
    document.addToField("field1", value1);
    document.addToField("field1", value2);
    assertEquals(value1, document.getBytes("field1"));
    assertEquals(Arrays.asList(value1, value2), document.getBytesList("field1"));
  }

  @Test
  public void testRemoveChildren() {
    Document doc = createDocument("doc");
    assertFalse(doc.hasChildren());
    doc.addChild(createDocument("child1"));
    assertTrue(doc.hasChildren());
    assertEquals(doc.getChildren().size(), 1);
    doc.addChild(createDocument("child2"));
    assertEquals(doc.getChildren().size(), 2);
    doc.removeChildren();
    // validate no children exist after we remove them.
    assertFalse(doc.hasChildren());
  }

  @Test
  public void testGetAndSetNestedJson() {
    ObjectMapper mapper = new ObjectMapper();
    Document document = createDocument("doc");

    // test nested field that does not contain a list
    JsonNode jsonNode = mapper.createObjectNode()
        .set("b", mapper.createObjectNode()
            .set("c", mapper.createObjectNode()
                .set("d", mapper.createObjectNode()
                    .put("e", 234))));
    document.setNestedJson(("a.b.c.d"), mapper.createObjectNode().put("e", 234));
    assertEquals(jsonNode, document.getJson("a"));
    assertEquals(234, document.getNestedJson("a.b.c.d.e").asInt());

    // test nested field that contains a list in a top level field
    JsonNode jsonList = mapper.createArrayNode()
        .add(mapper.createObjectNode()
                .put("name", "thing1")
                .put("age", 50))
        .add(mapper.createObjectNode()
                .put("name", "thing2")
                .put("age", 25));
    document.setNestedJson("list[0]", mapper.createObjectNode().put("name", "temp").put("age", 87));
    document.setNestedJson("list[1]", mapper.createObjectNode().put("name", "thing2").put("age", 25));
    document.setNestedJson("list[0]", mapper.createObjectNode().put("name", "thing1").put("age", 50));
    assertEquals(jsonList, document.getJson("list"));
    assertEquals("thing2", document.getNestedJson("list[1].name").asText());

    // test nested field with a list that is more nested
    JsonNode moreNestedJsonList = mapper.createObjectNode().set("list", mapper.createArrayNode()
        .add(mapper.createObjectNode()
            .put("name", "thing1")
            .put("age", 50))
        .add(mapper.createObjectNode()
            .put("name", "thing2")
            .put("age", 25)
            .set("phone", mapper.createObjectNode()
                .put("number", "123-4567")
                .put("areaCode", "303"))));
    document.setNestedJson("doc.list[0]", mapper.createObjectNode().put("name", "thing1").put("age", 50));
    document.setNestedJson("doc.list[1]", mapper.createObjectNode().put("name", "thing2").put("age", 25));
    document.setNestedJson("doc.list[1].phone", mapper.createObjectNode().put("number", "123-4567").put("areaCode", "303"));
    assertEquals(moreNestedJsonList, document.getJson("doc"));
    assertEquals("thing2", document.getNestedJson("doc.list[1].name").asText());

    // test non nested field
    document.setNestedJson("simple", mapper.createObjectNode().put("field", "simpleValue"));
    assertEquals("simpleValue", document.getNestedJson("simple.field").asText());

    // test non-existing field
    assertNull(document.getNestedJson("a.does.not.exist"));
  }

  @Test
  public void testNestedArrayUpdate() {
    Document doc = Document.create("id1");
    assertThrows(ArrayIndexOutOfBoundsException.class, () -> doc.setNestedJson("a.b[1]", IntNode.valueOf(50))); // a.b doesn't exist, this will throw an error
    doc.setNestedJson("a.b[0]", IntNode.valueOf(50)); // a.b still doesn't exist, it will create a new array and add as the first element
    doc.setNestedJson("a.b[0]", IntNode.valueOf(60)); // a.b.0 exists, this will update the value at index 0
    assertThrows(ArrayIndexOutOfBoundsException.class, () -> doc.setNestedJson("a.b[20]", IntNode.valueOf(50))); // a.b does not have 20 or more values, this will throw an error
    doc.setNestedJson("a.b[1]", IntNode.valueOf(50)); // now that a.b has one value, this will be added as the next value with index 1
    assertEquals(2, doc.getNestedJson("a.b").size());
    assertEquals(60, doc.getNestedJson("a.b[0]").intValue());
    assertEquals(50, doc.getNestedJson("a.b[1]").intValue());
    assertNull(doc.getNestedJson("a.b[2]"));
  }

  @Test
  public void testDeeplyNestedArrayUpdate() {
    Document doc = Document.create("id1");
    doc.setNestedJson("a.b.c[0].d[0][0][0].e[0][0]", IntNode.valueOf(101));
    assertEquals(101, doc.getNestedJson("a.b.c[0].d[0][0][0].e[0][0]").intValue());
    assertEquals(doc.getJson("a").toString(), "{\"b\":{\"c\":[{\"d\":[[[{\"e\":[[101]]}]]]}]}}");
  }

  @Test
  public void testRemoveNestedJson() {
    ObjectMapper mapper = new ObjectMapper();
    Document document = createDocument("doc");

    document.setNestedJson("a.b.c", IntNode.valueOf(1));
    document.setNestedJson("a.b.d", IntNode.valueOf(2));
    document.setNestedJson("a.list", mapper.createArrayNode().add(10).add(20).add(30));
    document.setNestedJson("a.meta", mapper.createObjectNode().put("flag", true));

    document.removeNestedJson("a.b.c");
    assertNull(document.getNestedJson("a.b.c"));
    assertNotNull(document.getNestedJson("a.b.d"));
    assertTrue(document.getNestedJson("a.b").isObject());

    document.removeNestedJson("a.list[1]");
    JsonNode list = document.getNestedJson("a.list");
    assertNotNull(list);
    assertTrue(list.isArray());
    assertEquals(2, list.size());
    assertEquals(10, list.get(0).asInt());
    assertEquals(30, list.get(1).asInt());

    document.removeNestedJson("a.list");
    assertNull(document.getNestedJson("a.list"));
    assertTrue(document.getNestedJson("a").isObject());

    document.removeNestedJson("a.missing.child");
    assertNotNull(document.getNestedJson("a.b.d"));
    assertTrue(document.getNestedJson("a.meta").isObject());

    document.removeNestedJson("a.b");
    assertNull(document.getNestedJson("a.b"));
    JsonNode aNode = document.getJson("a");
    assertNotNull(aNode);
    assertTrue(aNode.isObject());
    assertNotNull(document.getNestedJson("a.meta"));
  }

  @Test
  public void testParseAndStringifyNestedPath() {
    // simple path
    assertEquals("a", Segment.stringify(Segment.parse("a")));

    // complex path
    String path = "a.b.c[5].d[4][6][7].e.f[4].x";
    List<Segment> segments = Segment.parse(path);
    assertEquals(path, Segment.stringify(segments));
  }

  @Test
  public void testParseIllegalSyntax() {
    assertThrows(Exception.class, () -> Segment.parse("a.b.c.[].d")); // empty []
    assertThrows(Exception.class, () -> Segment.parse("a.b.c.[")); // unbalanced [
    assertThrows(Exception.class, () -> Segment.parse("a.b.c.]")); // unbalanced ]
    assertThrows(Exception.class, () -> Segment.parse("a.b.c.[a]")); // non-integer index
    assertThrows(Exception.class, () -> Segment.parse("a.b.c.[.]")); // non-integer index
    assertThrows(Exception.class, () -> Segment.parse("a.b[[1]]")); // nested index
    assertThrows(Exception.class, () -> Segment.parse(" a")); // whitespace
    assertThrows(Exception.class, () -> Segment.parse("a ")); // whitespace
    assertThrows(Exception.class, () -> Segment.parse("a. b")); // whitespace
    assertThrows(Exception.class, () -> Segment.parse("[1]")); // index only
  }

  @Test
  public void testLenientParsing() {
    // these are some cases where Segment.parse() accepts syntax that is not legal javascript
    // we may want to tighten the parser to reject these cases, but they are corrected in predictable ways
    assertEquals("a", Segment.stringify(Segment.parse(".a"))); // initial dot ignored
    assertEquals("a", Segment.stringify(Segment.parse("a."))); // trailing dot ignored
    assertEquals("a.b", Segment.stringify(Segment.parse("a...b"))); // several dots in a row ignored
    assertEquals("a[1].b", Segment.stringify(Segment.parse("a[1]b"))); // missing dot after ] added
    assertEquals("a[1]", Segment.stringify(Segment.parse("a.[1]"))); // dot before [ ignored
  }

  @Test
  public void testValidateFieldNames() {

    Document doc = createDocument("doc");
    JsonNode node = new ObjectMapper().createObjectNode();

    // build a list of illegal field names
    List<String> illegalFieldNames = new ArrayList<>(RESERVED_FIELDS);
    illegalFieldNames.add(null);
    illegalFieldNames.add("");

    // build a list of functions that set, update, add, or remove fields
    List<BiConsumer<String, Document>> setFunctions = List.of((fieldName, document) -> document.setField(fieldName, "val"),
        (fieldName, document) -> document.setField(fieldName, 1L), (fieldName, document) -> document.setField(fieldName, 1),
        (fieldName, document) -> document.setField(fieldName, true), (fieldName, document) -> document.setField(fieldName, 1.0),
        (fieldName, document) -> document.setField(fieldName, 1.0f), (fieldName, document) -> document.setField(fieldName, node),
        (fieldName, document) -> document.setField(fieldName, Instant.now()),
        (fieldName, document) -> document.setField(fieldName, new byte[] {0x3c, 0x4c, 0x5c}));

    List<BiConsumer<String, Document>> updateFunctions =
        List.of((fieldName, document) -> document.update(fieldName, UpdateMode.DEFAULT, "val"),
            (fieldName, document) -> document.update(fieldName, UpdateMode.DEFAULT, 1L),
            (fieldName, document) -> document.update(fieldName, UpdateMode.DEFAULT, 1),
            (fieldName, document) -> document.update(fieldName, UpdateMode.DEFAULT, true),
            (fieldName, document) -> document.update(fieldName, UpdateMode.DEFAULT, 1.0),
            (fieldName, document) -> document.update(fieldName, UpdateMode.DEFAULT, 1.0f),
            // todo missing update for JsonNode
            // (fieldName, document)-> document.update(fieldName, UpdateMode.DEFAULT, node),
            (fieldName, document) -> document.update(fieldName, UpdateMode.DEFAULT, Instant.now()),
            (fieldName, document) -> document.update(fieldName, UpdateMode.DEFAULT, new byte[] {0x3c, 0x4c, 0x5c}));

    List<BiConsumer<String, Document>> addToFieldFunctions = List.of((fieldName, document) -> document.addToField(fieldName, "val"),
        (fieldName, document) -> document.addToField(fieldName, 1L), (fieldName, document) -> document.addToField(fieldName, 1),
        (fieldName, document) -> document.addToField(fieldName, true), (fieldName, document) -> document.addToField(fieldName, 1.0),
        (fieldName, document) -> document.addToField(fieldName, 1.0f),
        // (fieldName, document) -> document.addToField(fieldName, node),
        (fieldName, document) -> document.addToField(fieldName, Instant.now()),
        (fieldName, document) -> document.addToField(fieldName, new byte[] {0x3c, 0x4c, 0x5c}));

    List<BiConsumer<String, Document>> setOrAddFunctions = List.of((fieldName, document) -> document.setOrAdd(fieldName, "val"),
        (fieldName, document) -> document.setOrAdd(fieldName, 1L), (fieldName, document) -> document.setOrAdd(fieldName, 1),
        (fieldName, document) -> document.setOrAdd(fieldName, true), (fieldName, document) -> document.setOrAdd(fieldName, 1.0),
        (fieldName, document) -> document.setOrAdd(fieldName, 1.0f),
        // (fieldName, document) -> document.setOrAdd(fieldName, node),
        (fieldName, document) -> document.setOrAdd(fieldName, Instant.now()),
        (fieldName, document) -> document.setOrAdd(fieldName, new byte[] {0x3c, 0x4c, 0x5c}));

    List<BiConsumer<String, Document>> removeFunctions = List.of((fieldName, document) -> document.removeField(fieldName),
        (fieldName, document) -> document.removeFromArray(fieldName, 0));

    // merge lists of functions
    List<BiConsumer<String, Document>> functions = new ArrayList<>();
    functions.addAll(setFunctions);
    functions.addAll(updateFunctions);
    functions.addAll(addToFieldFunctions);
    functions.addAll(setOrAddFunctions);
    functions.addAll(removeFunctions);

    // test merged list of function
    for (String fieldName : illegalFieldNames) {
      for (BiConsumer<String, Document> function : functions) {
        assertThrows(IllegalArgumentException.class, () -> function.accept(fieldName, doc));
      }
    }

    // test renameField method
    doc.addToField("field1", "val");
    for (String fieldName : illegalFieldNames) {
      assertThrows(IllegalArgumentException.class, () -> doc.renameField("field1", fieldName, UpdateMode.DEFAULT));
    }
  }
}
