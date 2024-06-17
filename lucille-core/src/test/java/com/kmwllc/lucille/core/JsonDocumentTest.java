package com.kmwllc.lucille.core;

import com.api.jsonata4java.expressions.Expressions;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import org.junit.Test;

import java.util.Base64;
import java.util.List;
import java.util.function.UnaryOperator;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class JsonDocumentTest extends DocumentTest.NodeDocumentTest {

  @Override
  public Document createDocument(ObjectNode node) throws DocumentException {
    return new JsonDocument(node);
  }

  @Override
  public Document createDocument(String id) {
    return new JsonDocument(id);
  }

  @Override
  public Document createDocument(String id, String runId) {
    return new JsonDocument(id, runId);
  }

  @Override
  public Document createDocumentFromJson(String json, UnaryOperator<String> idUpdater)
      throws DocumentException, JsonProcessingException {
    return JsonDocument.fromJsonString(json, idUpdater);
  }

  @Test
  public void testNestedArrays() throws DocumentException, JsonProcessingException {
    Document d = createDocumentFromJson("{\"id\":\"id\",\"nested\":[[\"first\"],[\"second\"]]}");
    ObjectMapper mapper = new ObjectMapper();
    JsonNode first = mapper.readTree("[\"first\"]");
    JsonNode second = mapper.readTree("[\"second\"]");
    assertEquals(List.of(first, second), d.getJsonList("nested"));

    // here we're calling the wrong method, getStringList(), to retrieve a json array containing json arrays
    // we get back a list of empty strings because, ultimately, asText() is called on each JsonNode inside the outer array,
    // and in this case, asText() returns "" for non-text values;
    // this is known behavior and is not considered a problem: the various getters on Document make a best effort to
    // return a properly converted value, but ultimately it is the caller's responsibility to know which
    // getter to call for the type of data stored in a given field
    assertEquals(List.of("", ""), d.getStringList("nested"));
  }

  @Test
  public void testByteArraySerialization() throws Exception {
    byte[] value1 = new byte[]{0x3c, 0x4c, 0x5c, 0x3c, 0x4c, 0x5c, 0x3c, 0x4c, 0x5c};

    Document document = createDocument("doc");
    document.setField("field1", value1);
    assertArrayEquals(value1, document.getBytes("field1"));

    // a byte array should be serialized as a base64-encoded string
    assertTrue(document.toString().contains(Base64.getEncoder().encodeToString(value1)));

    Document document2 = createDocumentFromJson(document.toString());
    assertArrayEquals(value1, document2.getBytes("field1"));
  }

  @Test
  public void testByteArraySerializationMultivalued() throws Exception {
    byte[] value1 = new byte[]{0x3c, 0x4c, 0x5c};
    byte[] value2 = new byte[]{0x4c, 0x4c, 0x5c};
    Document document = createDocument("doc");
    document.addToField("field1", value1);
    document.addToField("field1", value2);

    // a byte array should be serialized as a base64-encoded string
    assertTrue(document.toString().contains(Base64.getEncoder().encodeToString(value1)));
    assertTrue(document.toString().contains(Base64.getEncoder().encodeToString(value2)));

    Document document2 = createDocumentFromJson(document.toString());
    assertArrayEquals(value1, document2.getBytesList("field1").get(0));
    assertArrayEquals(value2, document2.getBytesList("field1").get(1));
  }

  /**
   * Demonstrates that JsonDocument allows values of different types to be added
   * to the same multivalued field; the list of values can then be retrieved with a type-specific
   * getter (i.e. getStringList()) and type conversions will be performed as appropriate.
   *
   * The behavior of multivalued field on JsonDocument reflects the behavior of JsonArray,
   * which JsonDocument uses to represent such fields. In turn, this behavior matches the way
   * JSON itself works: arrays can contain elements of mixed type.
   *
   */
  @Test
  public void testAddMixedTypesToMultivaluedField() {
    Document doc = createDocument("id1");

    doc.addToField("field1", "my string");
    doc.addToField("field1", 1L);
    doc.addToField("field1", 2);
    doc.addToField("field1", true);
    doc.addToField("field1", 3D);
    doc.addToField("field1", 4.0F);
    doc.addToField("field1", Instant.parse("2024-03-12T16:09:32.231262Z"));
    doc.addToField("field1", new byte[] {});

    String[] expectedStringValues = {"my string", "1", "2", "true", "3.0", "4.0", "2024-03-12T16:09:32.231262Z", ""};
    assertArrayEquals(expectedStringValues, doc.getStringList("field1").toArray());
  }

  /**
   * Demonstrates that JsonDocument allows a value to be added as one type and retrieved as a different type.
   */
  @Test
  public void testTypeConversion() {
    Document doc = createDocument("id1");

    doc.setField("myIntField", 1);
    doc.setField("myStringField", "2");
    assertEquals(Integer.class, doc.asMap().get("myIntField").getClass());
    assertEquals(String.class, doc.asMap().get("myStringField").getClass());

    assertEquals("1", doc.getString("myIntField"));
    assertEquals("2", doc.getString("myStringField"));
    assertEquals(Integer.valueOf(1), doc.getInt("myIntField"));
    assertEquals(Integer.valueOf(2), doc.getInt("myStringField"));

    assertEquals("1", doc.getStringList("myIntField").get(0));
    assertEquals("2", doc.getStringList("myStringField").get(0));
    assertEquals(Integer.valueOf(1), doc.getIntList("myIntField").get(0));
    assertEquals(Integer.valueOf(2), doc.getIntList("myStringField").get(0));
  }

  @Test
  public void testJsonHandling() throws Exception {

    ObjectMapper mapper = new ObjectMapper();

    JsonNode jsonObject1 = mapper.readTree("{\"a\":1, \"b\": 2}");
    JsonNode jsonObject2 = mapper.readTree("{\"c\":3, \"d\": 4}");
    JsonNode jsonArray1 = mapper.readTree("[{\"a\":1}, {\"b\": 2}]");
    List<JsonNode> jsonArray1AsList =
        List.of(mapper.readTree("{\"a\":1}"), mapper.readTree("{\"b\": 2}"));

    Document d = createDocument("id1");
    d.setField("jsonObject1", jsonObject1);
    d.setField("jsonArray1", jsonArray1);

    assertEquals(jsonObject1.deepCopy(), d.getJson("jsonObject1"));
    assertEquals(List.of(jsonObject1.deepCopy()), d.getJsonList("jsonObject1"));

    assertEquals(jsonArray1.deepCopy(), d.getJson("jsonArray1"));
    // if a JsonArray has been added as a single value and we retrieve it as a list, we get a list of the contents,
    // not a singleton list containing that array
    assertEquals(jsonArray1AsList, d.getJsonList("jsonArray1"));
    assertNotEquals(List.of(jsonArray1), d.getJsonList("jsonArray1"));

    assertFalse(d.isMultiValued("jsonObject1"));
    // even if a JsonArray was added as a single value, it will be considered as a multivalued because
    // JsonDocument represents any multivalued field using a JsonArray
    assertTrue(d.isMultiValued("jsonArray1"));


    d.addToField("jsonObject1", jsonObject2);
    assertTrue(d.isMultiValued("jsonObject1"));

    JsonNode jsonArray1WithObject2Added = mapper.readTree("[{\"a\":1}, {\"b\": 2}, {\"c\":3, \"d\": 4}]");
    d.addToField("jsonArray1", jsonObject2);
    assertEquals(jsonArray1WithObject2Added, d.getJson("jsonArray1"));

    JsonNode arrayOfObject1AndObject2 = mapper.readTree("[{\"a\":1, \"b\": 2}, {\"c\":3, \"d\": 4}]");
    d.setField("jsonObject1", jsonObject1);
    d.addToField("jsonObject1", jsonObject2);
    assertEquals(arrayOfObject1AndObject2, d.getJson("jsonObject1"));
    assertEquals(List.of(jsonObject1, jsonObject2), d.getJsonList("jsonObject1"));

    assertEquals(d, createDocumentFromJson(d.toString()));
    assertEquals(d.toString(), createDocumentFromJson(d.toString()).toString());

    // a field that was set as an int or other type can be retrieved as json
    d.setField("myIntField", 1);
    assertEquals(mapper.readTree("1"), d.getJson("myIntField"));
    assertEquals(1, d.getJson("myIntField").asInt());
    assertEquals("1", d.getJson("myIntField").toString());

    d.setOrAdd("myJson", mapper.readTree("{\"a\":1}"));
    assertEquals(mapper.readTree("{\"a\":1}"), d.getJson("myJson"));
    d.setOrAdd("myJson", mapper.readTree("{\"a\":2}"));
    assertEquals(mapper.readTree("[{\"a\":1}, {\"a\":2}]"), d.getJson("myJson"));
    d.setOrAdd("myJson", mapper.readTree("{\"a\":3}"));
    assertEquals(mapper.readTree("[{\"a\":1}, {\"a\":2}, {\"a\":3}]"), d.getJson("myJson"));
    d.update("myJson", UpdateMode.APPEND, mapper.readTree("{\"a\":4}"));
    assertEquals(mapper.readTree("[{\"a\":1}, {\"a\":2}, {\"a\":3}, {\"a\":4}]"), d.getJson("myJson"));
  }

  @Test
  public void testTransform() throws Exception {
    Document doc = createDocumentFromJson("{\"id\":\"id\",\"foo\": \"bar\"}");
    doc.setField("bytes", new byte[]{1, 2, 3});
    
    // test mutating a reserved field
    Expressions mutateReservedExpr = Expressions.parse("{\"id\":\"diff\",\"foo\": \"bar\"}");
    assertThrows(DocumentException.class, () -> doc.transform(mutateReservedExpr));
    assertEquals(3, doc.getFieldNames().size());
    assertEquals("id", doc.getId());
    assertEquals("bar", doc.getString("foo"));
    assertArrayEquals(new byte[]{1, 2, 3}, doc.getBytes("bytes"));

    // test mutatation does not create object 
    Expressions mutateIntoArray = Expressions.parse("[1, 2, 3]");
    Expressions mutateIntoNum = Expressions.parse("3");
    assertThrows(DocumentException.class, () -> doc.transform(mutateIntoArray));
    assertThrows(DocumentException.class, () -> doc.transform(mutateIntoNum));
    assertEquals(3, doc.getFieldNames().size());
    assertEquals("id", doc.getId());
    assertEquals("bar", doc.getString("foo"));
    assertArrayEquals(new byte[]{1, 2, 3}, doc.getBytes("bytes"));

    // test valid mutation 
    Expressions mutateFoo = Expressions.parse("$merge([$, {\"foo\": $substring(foo, 2)}])");
    doc.transform(mutateFoo);
    assertEquals(3, doc.getFieldNames().size());
    assertEquals("id", doc.getId());
    assertEquals("r", doc.getString("foo"));
    assertArrayEquals(new byte[]{1, 2, 3}, doc.getBytes("bytes"));
  }
}
