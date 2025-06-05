package com.kmwllc.lucille.core.spec;

import com.typesafe.config.Config;

// TODO: Not sure if this is something we'd want to include in a final version. I made it to temporarily not need
//  a full rewrite of every Spec, but not sure if there is a property that is of multiple types, sometimes.
public class AnyProperty extends Property {

  public AnyProperty(String name, boolean required) {
    super(name, required, null);
  }

  @Override
  protected void validatePresentProperty(Config config) { }
}
