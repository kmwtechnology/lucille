package com.kmwllc.lucille.core.spec;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

public class StringProperty extends Property {

  public StringProperty(String name, boolean required) {
    this(name, required, null);
  }

  public StringProperty(String name, boolean required, String description) {
    super(name, required, description);
  }

  @Override
  protected ObjectNode typeJson() {
    ObjectNode node = MAPPER.createObjectNode();

    node.put("type", "STRING");

    return node;
  }

  @Override
  protected void validatePresentProperty(Config config) {
    try {
      config.getString(name);
    } catch (ConfigException e) {
      throw new IllegalArgumentException(name + " must be a string, was \"" + config.getValue(name).valueType() + "\"");
    }
  }
}
