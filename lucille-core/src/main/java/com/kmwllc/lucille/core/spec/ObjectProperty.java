package com.kmwllc.lucille.core.spec;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

class ObjectProperty extends Property {

  private final TypeReference<?> typeReference;
  private final Spec parentSpec;

  public ObjectProperty(Spec parentSpec, boolean required) {
    super(requireParentName(parentSpec), required);

    this.parentSpec = parentSpec;
    this.typeReference = null;
  }

  public ObjectProperty(String name, boolean required, TypeReference<?> type) {
    super(name, required);

    this.parentSpec = null;
    this.typeReference = type;
  }

  private static String requireParentName(Spec parentSpec) {
    if (parentSpec.getName() == null) {
      throw new IllegalArgumentException("A parent property needs a Spec created with SpecBuilder.parent(name), which gives "
          + "the parent its name; this Spec has no name.");
    }
    return parentSpec.getName();
  }

  @Override
  protected ObjectNode typeJson() {
    ObjectNode node = MAPPER.createObjectNode();

    node.put("type", "OBJECT");

    if (parentSpec != null) {
      node.set("child", parentSpec.toJson());
    }

    if (typeReference != null) {
      node.put("typeReference", typeReference.getType().toString());
    }

    return node;
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
      // allowing the exception to be thrown
      parentSpec.validate(childConfig, name);
    }

    if (typeReference != null) {
      try {
        MAPPER.convertValue(childConfig.root().unwrapped(), typeReference);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Object " + name + " could not be converted to specified type: " + e.getMessage());
      }
    }
  }
}