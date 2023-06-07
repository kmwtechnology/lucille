package com.kmwllc.lucille.core;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.message.KafkaDocumentDeserializer;
import com.kmwllc.lucille.message.KafkaDocumentSerializer;
import org.apache.commons.lang.SerializationUtils;
import org.junit.Ignore;
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
  public void testByteArraySerializationWithTyping() throws Exception {
    byte[] value1 = new byte[]{ 0x3c, 0x4c, 0x5c };

    HashMapDocument document = new HashMapDocument("doc");
    document.setField("bytes", value1);

    String serialized = document.toTypedJsonString();

    HashMapDocument document2 = HashMapDocument.fromTypedJsonString(serialized, null);
    assertArrayEquals(value1, document2.getBytes("bytes"));
    assertEquals(byte[].class, document2.asMap().get("bytes").getClass());
  }

  // demonstrate that serialization/deserialization with untyped json doesn't work
  @Test(expected = ClassCastException.class)
  public void testByteArraySerialization() throws Exception {
    byte[] value1 = new byte[]{ 0x3c, 0x4c, 0x5c };
    HashMapDocument document = (HashMapDocument)createDocument("doc");
    document.setField("bytes", value1);
    Document document2 = createDocumentFromJson(document.toString(), null);
    assertArrayEquals(value1, document2.getBytes("bytes"));
    assertEquals(byte[].class, document2.asMap().get("bytes").getClass());
  }

  @Test
  public void testFloatSerializationWithTyping() throws Exception {
    Document document = createDocument("doc1");
    document.setField("field1", (Float)1.01F);
    assertEquals(Float.class, document.asMap().get("field1").getClass());
    Document document2 = HashMapDocument.fromTypedJsonString(document.toTypedJsonString(), null);
    assertEquals(Float.class, document2.asMap().get("field1").getClass());
    assertEquals((Float)1.01F, document2.getFloat("field1"));
  }

  @Test(expected = AssertionError.class)
  public void testNestedArrays() throws DocumentException, JsonProcessingException {
    // todo decide how this should be addressed
    Document d = createDocumentFromJson("{\"id\":\"id\",\"nested\":[[\"first\"],[\"second\"]]}");
    assertEquals(List.of(List.of("first"), List.of("second")), d.getStringList("nested"));
  }
}
