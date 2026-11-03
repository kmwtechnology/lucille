package com.kmwllc.lucille.core.spec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

public class SpecTest {

  public static class WithSpec {
    public static final Spec SPEC = SpecBuilder.withoutDefaults().requiredString("message").build();
  }

  public static class SubclassWithoutSpec extends WithSpec {
  }

  public static class SubclassWithRedeclaredSpec extends WithSpec {
    public static final Spec SPEC = WithSpec.SPEC;
  }

  public static class WithNullSpec {
    public static final Spec SPEC = null;
  }

  public static class WithPrivateSpec {
    private static final Spec SPEC = SpecBuilder.withoutDefaults().build();
  }

  public static class WithNonSpecField {
    public static final String SPEC = "not a spec";
  }

  @Test
  public void testOf() {
    assertSame(WithSpec.SPEC, Spec.of(WithSpec.class));
    assertSame(WithSpec.SPEC, Spec.of(SubclassWithRedeclaredSpec.class));

    for (Class<?> c : new Class<?>[] {SubclassWithoutSpec.class, WithNullSpec.class, WithPrivateSpec.class, WithNonSpecField.class}) {
      IllegalStateException e = assertThrows(IllegalStateException.class, () -> Spec.of(c));
      assertTrue(e.getMessage().contains(c.getName()));
      assertTrue(e.getMessage().contains("not on its superclasses"));
    }
  }

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

  @Test
  public void testStringOrListProperty() {
    Spec spec = SpecBuilder.withoutDefaults().requiredStringOrList("url").build();

    assertEquals("STRING_OR_LIST", spec.toJson().get("fields").get(0).get("type").get("type").asText());

    spec.validate(ConfigFactory.parseString("url: \"http://a:9200\""), "test");
    spec.validate(ConfigFactory.parseString("url: [\"http://a:9200\", \"http://b:9200\"]"), "test");

    assertThrows(IllegalArgumentException.class, () -> spec.validate(ConfigFactory.parseString("url: []"), "test"));
    assertThrows(IllegalArgumentException.class, () -> spec.validate(ConfigFactory.parseString("url: [{a: 1}]"), "test"));
    assertThrows(IllegalArgumentException.class, () -> spec.validate(ConfigFactory.parseString("url: {a: 1}"), "test"));
    assertThrows(IllegalArgumentException.class, () -> spec.validate(ConfigFactory.parseString("other: 1"), "test"));
  }

}
