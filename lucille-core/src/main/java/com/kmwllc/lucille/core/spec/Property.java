package com.kmwllc.lucille.core.spec;

import com.typesafe.config.Config;

/**
 * A property in a Config. Properties have a certain type, a name, and are either required or optional.
 */
public abstract class Property {

  protected final String name;
  protected final boolean required;
  private final String description;

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
   * Validate that a property has the correct value / type, given that it is present in the given Config.
   * @param config A config known to contain a path with this property's name.
   */
  protected abstract void validatePresentProperty(Config config);
}
