package com.kmwllc.lucille.core.spec;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueType;
import org.junit.Test;

public class ListPropertyTest {

  @Test
  public void testBasicTypes() {
    Config config = ConfigFactory.parseResourcesAnySyntax("PropertyTest/ListProperty/numList.conf");
    Property property = new ListProperty("list", true, ConfigValueType.NUMBER);
    property.validate(config);

    config = ConfigFactory.parseResourcesAnySyntax("PropertyTest/ListProperty/boolList.conf");
    property = new ListProperty("list", true, ConfigValueType.BOOLEAN);
    property.validate(config);

    config = ConfigFactory.parseResourcesAnySyntax("PropertyTest/ListProperty/stringList.conf");
    property = new ListProperty("list", true, ConfigValueType.STRING);
    property.validate(config);

    config = ConfigFactory.parseResourcesAnySyntax("PropertyTest/ListProperty/configList.conf");
    property = new ListProperty("list", true, ConfigValueType.OBJECT);
    property.validate(config);

    config = ConfigFactory.parseResourcesAnySyntax("PropertyTest/ListProperty/listOfList.conf");
    property = new ListProperty("list", true, ConfigValueType.LIST);
    property.validate(config);

    assertThrows(IllegalArgumentException.class, () -> new ListProperty("list", true, ConfigValueType.NULL));
  }

  @Test
  public void testEmptyList() {
    Config config = ConfigFactory.parseResourcesAnySyntax("PropertyTest/ListProperty/emptyList.conf");
    Property property = new ListProperty("list", true, ConfigValueType.NUMBER);
    property.validate(config);

    property = new ListProperty("list", true, ConfigValueType.BOOLEAN);
    property.validate(config);

    property = new ListProperty("list", true, ConfigValueType.STRING);
    property.validate(config);

    property = new ListProperty("list", true, ConfigValueType.OBJECT);
    property.validate(config);

    property = new ListProperty("list", true, ConfigValueType.LIST);
    property.validate(config);
  }

  @Test
  public void testTypeConversions() {
    Config config = ConfigFactory.parseResourcesAnySyntax("PropertyTest/ListProperty/listOfStringsThatAreNumbers.conf");
    Property property = new ListProperty("list", true, ConfigValueType.NUMBER);
    property.validate(config);

    // should be able to read the list of numbers as a list of strings
    config = ConfigFactory.parseResourcesAnySyntax("PropertyTest/ListProperty/numList.conf");
    property = new ListProperty("list", true, ConfigValueType.STRING);
    property.validate(config);
  }

  @Test
  public void testBadTypes() {
    Config config = ConfigFactory.parseResourcesAnySyntax("PropertyTest/ListProperty/boolList.conf");
    Property property = new ListProperty("list", true, ConfigValueType.NUMBER);
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> property.validate(config));

    // message should be complaining about the type
    assertTrue(e.getMessage().contains("supposed to be a list of"));

    Config justStringConfig = ConfigFactory.parseResourcesAnySyntax("PropertyTest/ListProperty/listIsJustAString.conf");
    e = assertThrows(IllegalArgumentException.class, () -> property.validate(justStringConfig));

    // message should be complaining about not a list
    assertTrue(e.getMessage().contains("supposed to be a list, was \"STRING\""));
  }

}
