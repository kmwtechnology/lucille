package com.kmwllc.lucille.core.spec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Specifications for a Config and which properties it is allowed / needs to have for a certain piece of Lucille code, typically
 * a Connector, Stage, or Indexer. Properties are either required or optional, and they have a certain type.
 * The three basic types are number, boolean, and string. You can also specify a list or an objectc. For these types, you need to either
 * provide a Spec or a TypeReference describing the list or object.
 *
 * <p> For an object, providing a named <code>Spec</code> (created via SpecBuilder.parent())
 * describes the object's key and the properties it can / must have.
 * Providing a <code>TypeReference</code> describes what the unwrapped ConfigObject should deserialize as / cast to. For example, if you
 * need a "field mapping" that maps Strings to Strings, you should pass in <code>TypeReference&lt;Map&lt;String, String&gt;&gt;</code>.
 *
 * <p> For a list, you should provide a <code>Spec</code> when you need a list of Configs with a specific set of properties. A common
 * example is conditions for a Stage. Provide a <code>TypeReference</code> otherwise, describing what the unwrapped ConfigList should deserialize as
 * / cast to. For example, if you need a List of Doubles, you should pass in <code>TypeReference&lt;List&lt;Double&gt;&gt;</code>.
 *
 * <p> Generally, for objects and lists, use a Spec when you know field names in advance, and a <code>TypeReference</code> when you do not.
 *
 * <p>A Spec is immutable and should be created using SpecBuilder.</p>
 */
public class Spec {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String name;

  private final Set<Property> properties;

  Spec(String name, Set<Property> defaultLegalProperties) {
    this.name = name;
    this.properties = new HashSet<>(defaultLegalProperties);
  }

  Spec(Set<Property> defaultLegalProperties) {
    this.name = null;
    this.properties = new HashSet<>(defaultLegalProperties);
  }

  /**
   * Convenience constructor to create a Spec without default legal properties.
   */
  Spec() {
    this(Set.of());
  }

  /**
   * Gets the spec's properties.
   * @return a copy of the set of properties.
   */
  Set<Property> getProperties() {
    return Set.copyOf(properties);
  }

  /**
   * Validates this Config using this Spec's properties. Throws an Exception if the Config is missing a required
   * parent / property or contains a non-legal property.
   * @param config The Config that you want to validate against this Spec's properties.
   * @throws IllegalArgumentException If the given config is either missing a required property / parent or contains
   * a non-legal property / parent.
   */
  public void validate(Config config, String displayName) {
    // mainly using a set to prevent repeats of any error messages
    Set<String> errorMessages = new HashSet<>();

    Set<String> keys = config.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toSet());

    // This method is responsible for making sure that there are no unknown / illegal field names found.
    // Properties are responsible for making sure a required field is present and for making sure that the
    // field has correct type.
    Set<String> legalProperties = getLegalProperties();

    for (String key : keys) {
      if (!legalProperties.contains(key)) {
        String parentName = getParent(key);
        if (parentName == null) {
          errorMessages.add("Config contains unknown property " + key);
        } else if (!legalProperties.contains(parentName)) {
          errorMessages.add("Config contains unknown parent " + parentName);
        }
      }
    }

    for (Property property : properties) {
      try {
        property.validate(config);
      } catch (IllegalArgumentException e) {
        errorMessages.add(e.getMessage());
      }
    }

    if (!errorMessages.isEmpty()) {
      if (errorMessages.size() == 1) {
        throw new IllegalArgumentException("Error with " + displayName + " Config: " + errorMessages.iterator().next());
      } else {
        throw new IllegalArgumentException("Errors with " + displayName + " Config: " + errorMessages);
      }

    }
  }

  /**
   * Returns a Set of the legal properties associated with this Spec, include required/optional properties/parents
   * and any default properties associated with the Spec as well. Note that, if this Spec has an {@link ObjectProperty}, this
   * will only return that property's name - <b>not</b> its child's legal properties as well.
   * @return a Set of the legal properties associated with this Spec.
   */
  public Set<String> getLegalProperties() {
    return properties.stream().map(Property::getName).collect(Collectors.toSet());
  }

  /**
   * Returns the String to the parent of the given Config property. Returns null if it doesn't exist.
   * @param property The property whose parent you want to get a Config path for.
   * @return A config path to the parent of the given property, null if it doesn't exist.
   */
  static String getParent(String property) {
    int dotIndex = property.indexOf('.');
    if (dotIndex < 0 || dotIndex == property.length() - 1) {
      return null;
    }
    return property.substring(0, dotIndex);
  }

  /**
   * Returns this Spec as a JsonNode describing the properties it declares. The returned node will contain one entry, <code>fields</code>,
   * which will be a list of JsonNode(s) describing the associated properties. See {@link Property#json()} for information on what these
   * objects will contain.
   * @return A JsonNode describing this Spec and the properties it declares.
   */
  public JsonNode toJson() {
    ObjectNode node = MAPPER.createObjectNode();

    ArrayNode fieldsArray = MAPPER.createArrayNode();

    for (Property prop : properties) {
      fieldsArray.add(prop.json());
    }

    node.set("fields", fieldsArray);

    return node;
  }

  /**
   * Returns the name of this spec if it is a "parent"; null otherwise.
   */
  public String getName() {
    return name;
  }
}
