package com.kmwllc.lucille.core.spec;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

public class BooleanProperty extends Property {

  public BooleanProperty(String name, boolean required) {
    super(name, required);
  }

  @Override
  protected void validatePresentProperty(Config config) {
    try {
      config.getBoolean(name);
    } catch (ConfigException e) {
      throw new IllegalArgumentException(name + " must be a boolean, was \"" + config.getValue(name).valueType() + "\"");
    }
  }
}