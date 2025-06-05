package com.kmwllc.lucille.core.spec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Test;

public class SpecTest {

  @Test
  public void testSerialize() {
    JsonNode messageJson = Spec.withoutDefaults().reqStrWithDesc("message", "A message to send.").serialize();
    JsonNode messageNode = messageJson.get("fields").get(0);

    assertEquals("message", messageNode.get("name").asText());
    assertTrue(messageNode.get("required").booleanValue());
    assertEquals("STRING", messageNode.get("type").asText());

    JsonNode withDescriptionJson = Spec.withoutDefaults().optStrWithDesc("message", "A message to send.").serialize();
    messageNode = withDescriptionJson.get("fields").get(0);

    assertEquals("message", messageNode.get("name").asText());
    assertFalse(messageNode.get("required").booleanValue());
    assertEquals("STRING", messageNode.get("type").asText());
    assertEquals("A message to send.", messageNode.get("description").asText());
  }

}
