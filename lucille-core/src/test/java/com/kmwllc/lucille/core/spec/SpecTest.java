package com.kmwllc.lucille.core.spec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Test;

public class SpecTest {

  @Test
  public void testToJson() {
    JsonNode messageJson = SpecBuilder.withoutDefaults().requiredStringWithDescription("message", "A message to send.").build().toJson();
    JsonNode messageNode = messageJson.get("fields").get(0);

    assertEquals("message", messageNode.get("name").asText());
    assertTrue(messageNode.get("required").asBoolean());
    assertEquals("STRING", messageNode.get("type").get("type").asText());

    JsonNode withDescriptionJson = SpecBuilder.withoutDefaults().optionalStringWithDescription("message", "A message to send.").build().toJson();
    messageNode = withDescriptionJson.get("fields").get(0);

    assertEquals("message", messageNode.get("name").asText());
    assertFalse(messageNode.get("required").asBoolean());
    assertEquals("STRING", messageNode.get("type").get("type").asText());
    assertEquals("A message to send.", messageNode.get("description").asText());
  }

}
