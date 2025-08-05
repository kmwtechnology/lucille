package com.kmwllc.lucille.core.spec;

import static org.junit.Assert.assertThrows;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

public class BooleanPropertyTest {

  @Test
  public void testBooleanValidation() {
    Property property = new BooleanProperty("field", true);

    Config config = ConfigFactory.parseResourcesAnySyntax("PropertyTest/boolean.conf");
    property.validate(config);

    config = ConfigFactory.parseResourcesAnySyntax("PropertyTest/stringThatIsABoolean.conf");
    property.validate(config);

    Config badConfig = ConfigFactory.parseResourcesAnySyntax("PropertyTest/int.conf");
    assertThrows(IllegalArgumentException.class, () -> property.validate(badConfig));
  }
}
