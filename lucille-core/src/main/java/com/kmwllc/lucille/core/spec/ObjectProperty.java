package com.kmwllc.lucille.core.spec;

import com.kmwllc.lucille.core.spec.Spec.ParentSpec;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

public class ObjectProperty extends Property {

  private final ParentSpec parentSpec;

  public ObjectProperty(ParentSpec parentSpec, boolean required) {
    this(parentSpec, required, null);
  }

  public ObjectProperty(String name, boolean required) {
    this(name, required, null);
  }

  public ObjectProperty(String name, boolean required, String description) {
    super(name, required, description);

    this.parentSpec = null;
  }

  public ObjectProperty(ParentSpec parentSpec, boolean required, String description) {
    super(parentSpec.getParentName(), required, description);

    this.parentSpec = parentSpec;
  }

  @Override
  protected void validatePresentProperty(Config config) {
    Config childConfig;

    try {
      childConfig = config.getConfig(name);
    } catch (ConfigException e) {
      throw new IllegalArgumentException(name + " is supposed to be a Config, was \"" + config.getValue(name).valueType() + "\"");
    }

    if (parentSpec != null) {
      parentSpec.validate(childConfig, name);
    }
  }
}