package com.kmwllc.lucille.core.spec;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import java.util.ArrayList;
import java.util.List;

public class ListProperty extends Property {

  private final Spec objectSpec;
  private final TypeReference<?> typeReference;

  public ListProperty(String name, boolean required, Spec objectSpec, String description) {
    super(name, required, description);

    this.objectSpec = objectSpec;
    this.typeReference = null;
  }

  public ListProperty(String name, boolean required, Spec objectSpec) {
    this(name, required, objectSpec, null);
  }

  public ListProperty(String name, boolean required, TypeReference<?> type, String description) {
    super(name, required, description);

    this.objectSpec = null;
    this.typeReference = type;
  }

  public ListProperty(String name, boolean required, TypeReference<?> type) {
    this(name, required, type, null);
  }

  @Override
  protected ObjectNode typeJson() {
    ObjectNode node = MAPPER.createObjectNode();

    node.put("type", "OBJECT");

    if (objectSpec != null) {
      node.set("child", objectSpec.toJson());
    }

    if (typeReference != null) {
      node.put("typeReference", typeReference.getType().toString());
    }

    return node;
  }

  @Override
  protected void validatePresentProperty(Config config) {
    // start by making sure the field is actually a list of some kind.
    try {
      config.getList(name);
    } catch (ConfigException e) {
      throw new IllegalArgumentException(name + " is supposed to be a list, was \"" + config.getValue(name).valueType() + "\"");
    }

    if (objectSpec != null) {
      List<? extends Config> configs;
      List<String> errorMessages = new ArrayList<>();

      try {
        configs = config.getConfigList(name);
      } catch (ConfigException e) {
        throw new IllegalArgumentException(name + " is supposed to be a list, was \"" + config.getValue(name).valueType() + "\"");
      }

      for (int i = 0; i < configs.size(); i++) {
        try {
          objectSpec.validate(configs.get(i), name + "[" + i + "]");
        } catch (IllegalArgumentException e) {
          errorMessages.add(e.getMessage());
        }
      }

      if (!errorMessages.isEmpty()) {
        if (errorMessages.size() == 1) {
          throw new IllegalArgumentException("Error with " + name + " element: " + errorMessages.get(0));
        } else {
          throw new IllegalArgumentException("Error with " + name + " elements: " + errorMessages);
        }
      }
    }

    if (typeReference != null) {
      try {
        MAPPER.convertValue(config.getList(name).unwrapped(), typeReference);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("List " + name + " could not be converted to specified type: " + e.getMessage());
      }
    }
  }
}
