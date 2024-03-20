package com.kmwllc.lucille.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import org.junit.Test;

import java.util.Base64;
import java.util.List;
import java.util.function.UnaryOperator;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

  @Test(expected = AssertionError.class)
  public void testNestedArrays() throws DocumentException, JsonProcessingException {
    // todo decide how this should be addressed
    Document d = createDocumentFromJson("{\"id\":\"id\",\"nested\":[[\"first\"],[\"second\"]]}");
    List<String> field = d.getStringList("nested"); // this returns a list with two empty strings
    assertEquals(List.of(List.of("first"), List.of("second")), d.getStringList("nested"));
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
}