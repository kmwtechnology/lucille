package com.kmwllc.lucille.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.UnaryOperator;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;


public class HashMapDocumentTest extends DocumentTest {

  @Override
  public Document createDocument(String id) {
    return new HashMapDocument(id);
  }

  @Override
  public Document createDocument(String id, String runId) {
    return new HashMapDocument(id, runId);
  }

  @Override
  public Document createDocument(ObjectNode node) throws DocumentException {
    return new HashMapDocument(node);
  }

  @Override
  public Document createDocumentFromJson(String json, UnaryOperator<String> idUpdater) throws DocumentException, JsonProcessingException {
    return HashMapDocument.fromJsonString(json, idUpdater);
  }

  @Test
  public void testGetBytesMissing() {
    Document document = createDocument("doc");
    assertNull(document.getBytes("field1"));
  }

  @Test
  public void testGetBytesSingleValued() {
    byte[] value = new byte[]{ 0x3c, 0x4c, 0x5c };
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
  public void testGetBytesListMultiValued() {
    byte[] value1 = new byte[]{ 0x3c, 0x4c, 0x5c };
    byte[] value2 = new byte[]{ 0x4c, 0x4c, 0x5c };
    byte[] value3 = new byte[]{ 0x5c, 0x4c, 0x5c };
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
    byte[] value1 = new byte[]{ 0x3c, 0x4c, 0x5c };
    byte[] value2 = new byte[]{ 0x4c, 0x4c, 0x5c };
    Document document = createDocument("doc");
    document.addToField("field1", value1);
    document.addToField("field1", value2);
    assertEquals(value1, document.getBytes("field1"));
    assertEquals(Arrays.asList(value1, value2), document.getBytesList("field1"));
  }

  @Test(expected = AssertionError.class)
  public void testNestedArrays() throws DocumentException, JsonProcessingException {
    // todo decide how this should be addressed
    Document d = createDocumentFromJson("{\"id\":\"id\",\"nested\":[[\"first\"],[\"second\"]]}");
    assertEquals(List.of(List.of("first"), List.of("second")), d.getStringList("nested"));
  }
}
