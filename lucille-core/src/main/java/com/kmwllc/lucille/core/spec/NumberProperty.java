package com.kmwllc.lucille.core.spec;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

public class NumberProperty extends Property {

  public NumberProperty(String name, boolean required) {
    super(name, required);
  }

  @Override
  protected void validatePresentProperty(Config config) {
    try {
      config.getNumber(name);
    } catch (ConfigException e) {
      throw new IllegalArgumentException(name + " must be a number, was \"" + config.getValue(name).valueType() + "\"");
    }
  }
}
