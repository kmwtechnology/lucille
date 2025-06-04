package com.kmwllc.lucille.core.spec;

import com.typesafe.config.Config;

public class AnyProperty extends Property {

  public AnyProperty(String name, boolean required) {
    super(name, required);
  }

  @Override
  protected void validatePresentProperty(Config config) { }
}
