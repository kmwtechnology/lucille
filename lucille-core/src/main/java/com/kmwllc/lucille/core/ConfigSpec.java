package com.kmwllc.lucille.core;

import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Specifications for a Config and which properties it is allowed / required to have.
 */
public class ConfigSpec {

  private final Set<String> defaultLegalProperties;

  private final Set<String> requiredProperties;
  private final Set<String> optionalProperties;
  private final Set<String> requiredParents;
  private final Set<String> optionalParents;

  private String displayName;

  private ConfigSpec(Set<String> defaultLegalProperties) {
    this.defaultLegalProperties = defaultLegalProperties;

    this.requiredProperties = new HashSet<>();
    this.optionalProperties = new HashSet<>();
    this.requiredParents = new HashSet<>();
    this.optionalParents = new HashSet<>();

    this.displayName = "Unknown";
  }

  /**
   * Creates a ConfigSpec with default legal properties suitable for a Stage. Includes name, class, conditions, and
   * conditionPolicy.
   * @return a ConfigSpec with default legal properties suitable for a Stage.
   */
  public static ConfigSpec forStage() {
    return new ConfigSpec(Set.of("name", "class", "conditions", "conditionPolicy"));
  }

  /**
   * Creates a ConfigSpec with default legal properties suitable for a Connector. Includes name, class, pipeline, docIdPrefix, and
   * collapse.
   * @return a ConfigSpec with default legal properties suitable for a Connector.
   */
  public static ConfigSpec forConnector() {
    return new ConfigSpec(Set.of("name", "class", "pipeline", "docIdPrefix", "collapse"));
  }


  /**
   * Returns this ConfigSpec with the given properties added as required properties.
   * @param properties The required properties you want to add to this ConfigSpec.
   * @return This ConfigSpec with the given required properties added.
   */
  public ConfigSpec withRequiredProperties(String... properties) {
    requiredProperties.addAll(Arrays.asList(properties));
    return this;
  }

  /**
   * Returns this ConfigSpec with the given properties added as optional properties.
   * @param properties The optional properties you want to add to this ConfigSpec.
   * @return This ConfigSpec with the given optional properties added.
   */
  public ConfigSpec withOptionalProperties(String... properties) {
    optionalProperties.addAll(Arrays.asList(properties));
    return this;
  }

  /**
   * Returns this ConfigSpec with the given parents added as required parents.
   * @param properties The required parents you want to add to this ConfigSpec.
   * @return This ConfigSpec with the given required parents added.
   */
  public ConfigSpec withRequiredParents(String... properties) {
    requiredParents.addAll(Arrays.asList(properties));
    return this;
  }

  /**
   * Returns this ConfigSpec with the given parents added as optional parents.
   * @param properties The optional parents you want to add to this ConfigSpec.
   * @return This ConfigSpec with the given optional parents added.
   */
  public ConfigSpec withOptionalParents(String... properties) {
    optionalParents.addAll(Arrays.asList(properties));
    return this;
  }

  /**
   * Sets this ConfigSpec to have the given display name.
   * @param newDisplayName The new display name for this ConfigSpec.
   */
  public void setDisplayName(String newDisplayName) {
    this.displayName = newDisplayName;
  }

  /**
   * Validates this Config using this ConfigSpec's properties. Throws an Exception if the Config is missing a required
   * parent / property or contains a non-legal property.
   * @param config The Config that you want to validate against this ConfigSpec's properties.
   */
  public void validate(Config config) {
    if (!disjoint(requiredProperties, optionalProperties, requiredParents, optionalParents)) {
      throw new IllegalArgumentException(displayName
          + ": Properties and parents sets must be disjoint.");
    }

    // verifies all required properties are present
    Set<String> keys = config.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toSet());
    for (String property : requiredProperties) {
      if (!keys.contains(property)) {
        throw new IllegalArgumentException(displayName + ": Config must contain property "
            + property);
      }
    }

    // verifies that
    // 1. all remaining properties are in the optional set or are nested;
    // 2. all required parents are present
    Set<String> observedRequiredParents = new HashSet<>();
    Set<String> legalProperties = mergeSets(requiredProperties, optionalProperties, defaultLegalProperties);
    for (String key : keys) {
      if (!legalProperties.contains(key)) {
        String parent = getParent(key);
        if (parent == null) {
          throw new IllegalArgumentException(displayName + ": Config contains unknown property "
              + key);
        } else if (requiredParents.contains(parent)) {
          observedRequiredParents.add(parent);
        } else if (!optionalParents.contains(parent)) {
          throw new IllegalArgumentException(displayName + ": Config contains unknown property "
              + key);
        }
      }
    }
    if (observedRequiredParents.size() != requiredParents.size()) {
      throw new IllegalArgumentException(displayName + ": Config is missing required parents: " +
          Sets.difference(requiredParents, observedRequiredParents));
    }
  }

  /**
   * Returns a Set of the legal properties associated with this ConfigSpec.
   * @return a Set of the legal properties associated with this ConfigSpec.
   */
  public Set<String> getLegalProperties() {
    return mergeSets(requiredProperties, optionalProperties, defaultLegalProperties);
  }

  // ***** Utility Functions *****

  /**
   * Returns whether the provided sets are disjoint. You must specify at least one set.
   *
   * @param sets The sets you want to check.
   * @return Whether the provided sets are disjoint.
   */
  @SafeVarargs
  static boolean disjoint(Set<String>... sets) {
    if (sets == null) {
      throw new IllegalArgumentException("Sets must not be null");
    }
    if (sets.length == 0) {
      throw new IllegalArgumentException("expecting at least one set");
    }

    Set<String> observed = new HashSet<>();
    for (Set<String> set : sets) {
      if (set == null) {
        throw new IllegalArgumentException("Each set must not be null");
      }
      for (String s : set) {
        if (observed.contains(s)) {
          return false;
        }
        observed.add(s);
      }
    }
    return true;
  }

  /**
   * Merges the given sets into one set.
   * @param sets The sets you want to merge together.
   * @return The sets merged together.
   * @param <T> The type of the sets you are merging together.
   */
  @SafeVarargs
  static <T> Set<T> mergeSets(Set<T>... sets) {
    Set<T> merged = new HashSet<>();
    for (Set<T> set : sets) {
      merged.addAll(set);
    }
    return Collections.unmodifiableSet(merged);
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
   * Validates the given config against the given properties, without any default legal properties.
   *
   * @param config The configuration you want to validate.
   * @param requiredProperties The properties you require in your config.
   * @param optionalProperties The properties allowed in your config.
   * @param requiredParents The parents you require in your config.
   * @param optionalParents The parents you allow in your config.
   */
  public static void validateConfig(Config config, List<String> requiredProperties, List<String> optionalProperties, List<String> requiredParents, List<String> optionalParents) {
    ConfigSpec spec = new ConfigSpec(Set.of())
        .withRequiredProperties(requiredProperties.toArray(new String[0]))
        .withOptionalProperties(optionalProperties.toArray(new String[0]))
        .withRequiredParents(requiredParents.toArray(new String[0]))
        .withOptionalParents(optionalParents.toArray(new String[0]));

    spec.validate(config);
  }
}
