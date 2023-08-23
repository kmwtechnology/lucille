package com.kmwllc.lucille.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;

import java.util.Base64;
import java.util.List;
import java.util.function.UnaryOperator;

import static org.junit.Assert.*;

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
  public Document createDocumentFromJson(String json, UnaryOperator<String> idUpdater) throws DocumentException, JsonProcessingException {
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
    byte[] value1 = new byte[]{ 0x3c, 0x4c, 0x5c, 0x3c, 0x4c, 0x5c, 0x3c, 0x4c, 0x5c };

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
    byte[] value1 = new byte[]{ 0x3c, 0x4c, 0x5c };
    byte[] value2 = new byte[]{ 0x4c, 0x4c, 0x5c };
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

  @Override
  public void testArrayNodeJsonNodeFieldSetEmpty() throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();

    JsonNode arrayNode = mapper.readTree("[]");
    Document d = createDocument("id1");
    String field = "myField";
    d.setField(field, arrayNode);

    // todo review these tests
    assertTrue(d.isMultiValued(field));
    // todo throws an error because since getSingleNode thinks its multivalued and tries to get the first element
    assertThrows(NullPointerException.class, () -> d.getJson(field));
    assertEquals(List.of(), d.getJsonList(field));
  }

  @Override
  public void testArrayNodeJsonNodeFieldSetFull() throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();

    JsonNode arrayNode = mapper.readTree("[{\"a\":1}, {\"b\": 2}]");
    Document d = createDocument("id1");
    String field = "myField";
    d.setField(field, arrayNode);

    // todo review these tests
    assertTrue(d.isMultiValued(field));
    // returns the first element of the array
    assertEquals(arrayNode.get(0), d.getJson(field));
    // for this to pass need to convert ArrayNode to List<JsonNode>
    assertEquals(List.of(arrayNode.get(0), arrayNode.get(1)), d.getJsonList(field));
  }
}