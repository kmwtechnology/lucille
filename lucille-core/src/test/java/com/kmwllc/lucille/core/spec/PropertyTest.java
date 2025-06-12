package com.kmwllc.lucille.core.spec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

public class PropertyTest {

  @Test
  public void testJson() {
    JsonNode propJson = new BooleanProperty("bool", true).json();
    assertEquals("bool", propJson.get("name").asText());
    assertTrue(propJson.get("required").asBoolean());

    // abstract method implemented by each subclass... won't test it here.
    assertTrue(propJson.has("type"));

    propJson = new NumberProperty("numRetries", false).json();
    assertEquals("numRetries", propJson.get("name").asText());
    assertFalse(propJson.get("required").asBoolean());
    assertTrue(propJson.has("type"));

    propJson = new StringProperty("systemPrompt", true, "Instructions to the LLM.").json();
    assertEquals("systemPrompt", propJson.get("name").asText());
    assertTrue(propJson.get("required").asBoolean());
    assertEquals("Instructions to the LLM.", propJson.get("description").asText());
    assertTrue(propJson.has("type"));
  }

  @Test
  public void testValidate() {
    Config config = ConfigFactory.parseResourcesAnySyntax("PropertyTest/string.conf");

    Property reqStringProperty = new StringProperty("field", true);
    Property reqMissingStringProperty = new StringProperty("NOT_HERE", true);
    Property optMissingStringProperty = new StringProperty("NOT_HERE", false);

    reqStringProperty.validate(config);
    optMissingStringProperty.validate(config);

    assertThrows(IllegalArgumentException.class, () -> reqMissingStringProperty.validate(config));
  }

}
