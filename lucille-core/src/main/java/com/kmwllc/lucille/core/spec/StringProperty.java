package com.kmwllc.lucille.core.spec;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

public class StringProperty extends Property {

  public StringProperty(String name, boolean required) {
    super(name, required);
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
