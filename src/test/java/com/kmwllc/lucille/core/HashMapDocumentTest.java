package com.kmwllc.lucille.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;

import java.util.function.UnaryOperator;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class HashMapDocumentTest extends DocumentTest {
  @Override
  public Document createDocument(ObjectNode node) throws DocumentException {
    return new HashMapDocument(node);
  }

  @Override
  public Document createDocument(String id) {
    return new HashMapDocument(id);
  }

  @Override
  public Document createDocument(String id, String runId) {
    return new HashMapDocument(id, runId);
  }

  @Override
  public Document createDocumentFromJson(String json, UnaryOperator<String> idUpdater) throws DocumentException, JsonProcessingException {
    return HashMapDocument.fromJsonString(json, idUpdater);
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
//    assertTrue(document3.isMultiValued("field1"));
    assertEquals("value1", document3.getString("field1"));
    document3.addToField("field1", "value2");
    assertTrue(document3.isMultiValued("field1"));
    assertEquals("value1", document3.getStringList("field1").get(0));
    assertEquals("value2", document3.getStringList("field1").get(1));
    assertEquals(2, document3.getStringList("field1").size());
  }
}
