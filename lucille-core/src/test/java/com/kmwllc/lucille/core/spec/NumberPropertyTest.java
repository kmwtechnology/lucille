package com.kmwllc.lucille.core.spec;

import static org.junit.Assert.assertThrows;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

public class NumberPropertyTest {

  @Test
  public void testNumberValidation() {
    Property property = new NumberProperty("field", true);

    Config config = ConfigFactory.parseResourcesAnySyntax("PropertyTest/int.conf");
    property.validate(config);

    config = ConfigFactory.parseResourcesAnySyntax("PropertyTest/double.conf");
    property.validate(config);

    config = ConfigFactory.parseResourcesAnySyntax("PropertyTest/stringThatIsANumber.conf");
    property.validate(config);

    Config badConfig = ConfigFactory.parseResourcesAnySyntax("PropertyTest/boolean.conf");
    assertThrows(IllegalArgumentException.class, () -> property.validate(badConfig));
  }

}
