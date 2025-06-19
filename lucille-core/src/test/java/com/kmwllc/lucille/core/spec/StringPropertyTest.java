package com.kmwllc.lucille.core.spec;

import static org.junit.Assert.assertThrows;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

public class StringPropertyTest {

  @Test
  public void testStringValidation() {
    Property property = new StringProperty("field", true);

    Config config = ConfigFactory.parseResourcesAnySyntax("PropertyTest/string.conf");
    property.validate(config);

    config = ConfigFactory.parseResourcesAnySyntax("PropertyTest/stringThatIsABoolean.conf");
    property.validate(config);

    config = ConfigFactory.parseResourcesAnySyntax("PropertyTest/int.conf");
    property.validate(config);

    config = ConfigFactory.parseResourcesAnySyntax("PropertyTest/boolean.conf");
    property.validate(config);

    Config listConfig = ConfigFactory.parseResourcesAnySyntax("PropertyTest/ListProperty/stringList.conf");
    assertThrows(IllegalArgumentException.class, () -> property.validate(listConfig));

    Config otherListConfig = ConfigFactory.parseResourcesAnySyntax("PropertyTest/ListProperty/listOfList.conf");
    assertThrows(IllegalArgumentException.class, () -> property.validate(otherListConfig));
  }

}
