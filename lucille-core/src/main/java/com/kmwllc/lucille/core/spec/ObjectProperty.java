package com.kmwllc.lucille.core.spec;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.core.spec.Spec.ParentSpec;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

public class ObjectProperty extends Property {

  private final TypeReference<?> typeReference;
  private final ParentSpec parentSpec;

  public ObjectProperty(ParentSpec parentSpec, boolean required) {
    this(parentSpec, required, null);
  }

  public ObjectProperty(String name, boolean required, TypeReference<?> type) {
    this(name, required, type, null);
  }

  public ObjectProperty(String name, boolean required, TypeReference<?> type, String description) {
    super(name, required, description);

    this.parentSpec = null;
    this.typeReference = type;
  }

  public ObjectProperty(ParentSpec parentSpec, boolean required, String description) {
    super(parentSpec.getParentName(), required, description);

    this.parentSpec = parentSpec;
    this.typeReference = null;
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