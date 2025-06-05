package com.kmwllc.lucille.core.spec;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigValueType;

public class ListProperty extends Property {

  private final ConfigValueType elementType;

  public ListProperty(String name, boolean required, ConfigValueType elementType) {
    this(name, required, elementType, null);
  }

  public ListProperty(String name, boolean required, ConfigValueType elementType, String description) {
    super(name, required, description);

    if (elementType == ConfigValueType.NULL) {
      throw new IllegalArgumentException("NULL is not a valid list type. Spec needs to be modified.");
    }

    this.elementType = elementType;
  }

  @Override
  protected void validatePresentProperty(Config config) {
    // start by making sure the field is actually a list of some kind.
    try {
      config.getList(name);
    } catch (ConfigException e) {
      throw new IllegalArgumentException(name + " is supposed to be a list, was \"" + config.getValue(name).valueType() + "\"");
    }

    // then check the actual element type of the list, if it is specified.
    if (elementType != null) {
      try {
        switch (elementType) {
          case NUMBER -> config.getNumberList(name);
          case OBJECT -> config.getConfigList(name);
          case STRING -> config.getStringList(name);
          case BOOLEAN -> config.getBooleanList(name);
          // if elementType is List, just allow it to be valid.
          // elementType won't be null - we prevent that above.
        }
      } catch (ConfigException e) {
        throw new IllegalArgumentException(
            name + " is supposed to be a list of " + elementType + ", is " + config.getList(name).valueType());
      }
    }
  }
}
