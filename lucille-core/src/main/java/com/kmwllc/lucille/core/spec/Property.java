package com.kmwllc.lucille.core.spec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;

/**
 * A property in a Config. Properties have a certain type, a name, and are either required or optional.
 */
public abstract class Property {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  protected final String name;
  protected final boolean required;
  protected final String description;

  public Property(String name, boolean required, String description) {
    this.name = name;
    this.required = required;
    this.description = description;
  }

  public String getName() { return name; }

  /**
   * Validates the given Config against this property. Returns an IllegalArgumentException if the given Config does not
   * satisfy this property. Returns null if there are no errors with the Config.
   *
   * @param config The configuration you want to ensure is compliant with this property.
   * @throws IllegalArgumentException For any errors the provided Config has with respect to this Property.
   */
  public final void validate(Config config) throws IllegalArgumentException {
    if (required && !config.hasPath(name)) {
      throw new IllegalArgumentException("Config is missing required property " + name);
    }

    // do subclass specific checks on the type
    if (config.hasPath(name)) {
      validatePresentProperty(config);
    }
  }

  /**
   * @return A description describing the purpose of this property. Will be null if no description was provided.
   */
  public String getDescription() {
    return description;
  }

  /**
   * Return a JsonNode describing this property. Includes <code>name</code>, the name of the field, <code>required</code>, whether it
   * is required, <code>description</code>, if it has one, and <code>type</code>, the type of the field.
   *
   * <p> <code>type</code> is one of <code>STRING</code>, <code>NUMBER</code>, <code>BOOLEAN</code>, <code>LIST</code>,
   * <code>OBJECT</code>, or <code>ANY</code> (TODO: May remove ANY.)
   */
  public JsonNode json() {
    ObjectNode node = MAPPER.createObjectNode();

    node.put("name", name);
    node.put("required", required);

    if (description != null) {
      node.put("description", description);
    }

    // TODO: Not the best way to get this information... Maybe subclasses aren't needed, *especially* if we don't type arrays
    // Object not written because it overrides this method
    if (this instanceof NumberProperty) {
      node.put("type", "NUMBER");
    } else if (this instanceof BooleanProperty) {
      node.put("type", "BOOLEAN");
    } else if (this instanceof StringProperty) {
      node.put("type", "STRING");
    } else if (this instanceof ListProperty) {
      node.put("type", "LIST");
    } else {
      node.put("type", "ANY");
    }

    return node;
  }

  /**
   * Validate that a property has the correct value / type, given that it is present in the given Config.
   * @param config A config known to contain a path with this property's name.
   */
  protected abstract void validatePresentProperty(Config config);
}
