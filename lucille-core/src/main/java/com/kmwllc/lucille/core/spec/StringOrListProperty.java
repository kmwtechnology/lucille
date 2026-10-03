package com.kmwllc.lucille.core.spec;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigValueType;

/**
 * A property that may be either a single String or a non-empty list of Strings. Useful for settings like a URL that
 * can optionally name several equivalent hosts.
 */
public class StringOrListProperty extends Property {

  public StringOrListProperty(String name, boolean required) {
    this(name, required, null);
  }

  public StringOrListProperty(String name, boolean required, String description) {
    super(name, required, description);
  }

  @Override
  protected ObjectNode typeJson() {
    ObjectNode node = MAPPER.createObjectNode();

    node.put("type", "STRING_OR_LIST");

    return node;
  }

  @Override
  protected void validatePresentProperty(Config config) {
    ConfigValueType type = config.getValue(name).valueType();

    if (type == ConfigValueType.LIST) {
      try {
        if (config.getStringList(name).isEmpty()) {
          throw new IllegalArgumentException(name + " must not be an empty list");
        }
      } catch (ConfigException e) {
        throw new IllegalArgumentException(name + " must be a list of strings");
      }
      return;
    }

    try {
      config.getString(name);
    } catch (ConfigException e) {
      throw new IllegalArgumentException(name + " must be a string or a list of strings, was \"" + type + "\"");
    }
  }
}
