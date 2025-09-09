package com.kmwllc.lucille.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.junit.Test;

public class JSONUtilsTest {
  private static final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testPutNestedValue() throws IOException {
    JsonNode node = mapper.createObjectNode()
        .set("top", mapper.createArrayNode()
            .add(mapper.createObjectNode().set("firstVal", mapper.createObjectNode().put("innerNode","field"))));
    String[] nestedKeys = {"top", "1", "intermediate", "inner"};
    JsonNode value = mapper.createObjectNode().put("evenMoreNested", "test");
    JSONUtils.putNestedFieldValue(node, nestedKeys, value);
    // check that node contains value node
    JsonNode finalNode = mapper.createObjectNode()
        .set("top", mapper.createArrayNode()
            .add(mapper.createObjectNode().set("firstVal", mapper.createObjectNode().put("innerNode","field")))
            .add(mapper.createObjectNode().set("intermediate", mapper.createObjectNode()
                .set("inner", mapper.createObjectNode().put("evenMoreNested", "test")))));
    assertEquals(finalNode, node);

    // test int value 1
    JsonNode anotherNode = mapper.createObjectNode()
        .set("intermediate", mapper.createObjectNode()
            .set("inner", mapper.createObjectNode()
                .put("innerNode","field")));
    JsonNode anotherValue = mapper.createObjectNode().put("evenMoreNested", "anotherValue");
    JSONUtils.putNestedFieldValue(anotherNode, nestedKeys, anotherValue, 2);
    JsonNode otherFinalNode = mapper.createObjectNode()
        .set("intermediate", mapper.createObjectNode()
            .set("inner", mapper.createObjectNode()
                .put("evenMoreNested", "anotherValue")));
    assertEquals(otherFinalNode, anotherNode);

    // test failure if start int val is too large
    assertThrows(IOException.class, () -> JSONUtils.putNestedFieldValue(node, nestedKeys, value, 4));
  }

  @Test
  public void testPutNestedStringValue() throws IOException {
    JsonNode value = mapper.convertValue("test", JsonNode.class);
    JsonNode node = mapper.createObjectNode()
        .set("top", mapper.createObjectNode()
            .set("intermediate", mapper.createObjectNode()));

    String[] nestedKeys = {"ahead", "top", "intermediate", "inner"};
    JSONUtils.putNestedFieldValue(node, nestedKeys, value, 1);
    // check that node contains value node
    JsonNode finalNode = mapper.createObjectNode()
        .set("top", mapper.createObjectNode()
            .set("intermediate", mapper.createObjectNode()
                .put("inner", "test")));
    assertEquals(finalNode, node);

    // test nested field length 1
    String[] nestedKey = {"oneField"};
    JSONUtils.putNestedFieldValue(node, nestedKey, value, 0);
    // check that node contains value node
    JsonNode anotherFinalNode = mapper.createObjectNode()
        .put("oneField", "test")
        .set("top", mapper.createObjectNode()
            .set("intermediate", mapper.createObjectNode()
                .put("inner", "test")));
    assertEquals(anotherFinalNode, node);

    // test failure if start int val is too large
    assertThrows(IOException.class, () -> JSONUtils.putNestedFieldValue(node, nestedKeys, value, 4));
  }


  @Test
  public void testGetNestedValue() throws IOException {
    String[] nestedKeys = {"ahead", "top", "intermediate", "inner"};
    JsonNode node = mapper.createObjectNode()
        .set("top", mapper.createObjectNode()
            .set("intermediate", mapper.createObjectNode().put("inner", "test")));
    assertEquals("test", JSONUtils.getNestedValue(nestedKeys, node, 1).textValue());

    // test null node
    assertNull(JSONUtils.getNestedValue(nestedKeys, mapper.createObjectNode()));
  }
}
