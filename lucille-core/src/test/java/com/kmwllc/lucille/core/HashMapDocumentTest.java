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
  public Document createDocumentFromJson(String json, UnaryOperator<String> idUpdater)
      throws DocumentException, JsonProcessingException {
    return HashMapDocument.fromJsonString(json, idUpdater);
  }

  @Test
  public void testByteArraySerializationWithDefaultTypingEnabled() throws Exception {
    ObjectMapper mapper =
        new ObjectMapper()
            .enableDefaultTyping(
                ObjectMapper.DefaultTyping.JAVA_LANG_OBJECT, JsonTypeInfo.As.PROPERTY);

    byte[] value1 = new byte[] {0x3c, 0x4c, 0x5c};

    HashMapDocument document = (HashMapDocument) createDocument("doc");
    document.setField("bytes", value1);

    String serialized = mapper.writeValueAsString(document);

    HashMapDocument document2 = mapper.readValue(serialized, HashMapDocument.class);
    assertArrayEquals(value1, document2.getBytes("bytes"));
    assertEquals(byte[].class, document2.asMap().get("bytes").getClass());
  }

  @Ignore
  @Test
  public void testByteArraySerialization() throws Exception {
    byte[] value1 = new byte[] {0x3c, 0x4c, 0x5c};
    HashMapDocument document = (HashMapDocument) createDocument("doc");
    document.setField("bytes", value1);
    Document document2 = createDocumentFromJson(document.toString(), null);
    assertArrayEquals(value1, document2.getBytes("bytes"));
    assertEquals(byte[].class, document2.asMap().get("bytes").getClass());
  }

  @Ignore
  @Test
  public void testFloatSerialization() throws Exception {
    Document document = createDocument("doc1");
    document.setField("field1", (Float) 1.01F);
    assertEquals(Float.class, document.asMap().get("field1").getClass());
    Document document2 = createDocumentFromJson(document.toString(), null);
    assertEquals(Float.class, document2.asMap().get("field1").getClass());
    assertEquals((Float) 1.01F, document2.getFloat("field1"));
  }

  @Test(expected = AssertionError.class)
  public void testNestedArrays() throws DocumentException, JsonProcessingException {
    // todo decide how this should be addressed
    Document d = createDocumentFromJson("{\"id\":\"id\",\"nested\":[[\"first\"],[\"second\"]]}");
    assertEquals(List.of(List.of("first"), List.of("second")), d.getStringList("nested"));
  }
}
