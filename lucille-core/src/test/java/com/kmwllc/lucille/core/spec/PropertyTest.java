package com.kmwllc.lucille.core.spec;

import static com.kmwllc.lucille.connector.FileConnector.S3_PARENT_SPEC;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Test;

public class PropertyTest {

  @Test
  public void testJson() {
    Property booleanProperty = new BooleanProperty("bool", true);
    assertEquals("{\"name\":\"bool\",\"required\":true,\"type\":\"BOOLEAN\"}", booleanProperty.json().toString());

    booleanProperty = new BooleanProperty("bool", false, "Important Property");
    assertEquals("{\"name\":\"bool\",\"required\":false,\"description\":\"Important Property\",\"type\":\"BOOLEAN\"}", booleanProperty.json().toString());

    Property listProperty = new ListProperty("list", false, null, "A list");
    assertEquals("{\"name\":\"list\",\"required\":false,\"description\":\"A list\",\"type\":\"LIST\"}", listProperty.json().toString());

    Property objectProperty = new ObjectProperty(S3_PARENT_SPEC, true);
    JsonNode objectPropertyJson = objectProperty.json();

    assertEquals(S3_PARENT_SPEC.toJson(), objectPropertyJson.get("child"));
  }

}
