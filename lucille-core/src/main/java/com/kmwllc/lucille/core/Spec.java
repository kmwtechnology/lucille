package com.kmwllc.lucille.core;

import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Specifications for a Config and which properties it is allowed / required to have.
 */
public class Spec {

  private final Set<String> defaultLegalProperties;

  private final Set<String> requiredProperties;
  private final Set<String> optionalProperties;

  private final Map<String, ParentSpec> requiredParentMap;
  private final Map<String, ParentSpec> optionalParentMap;

  private Spec(Set<String> defaultLegalProperties) {
    this.defaultLegalProperties = defaultLegalProperties;

    this.requiredProperties = new HashSet<>();
    this.optionalProperties = new HashSet<>();

    this.requiredParentMap = new HashMap<>();
    this.optionalParentMap = new HashMap<>();
  }

  /**
   * Creates a Spec with default legal properties suitable for a Stage. Includes name, class, conditions, and
   * conditionPolicy.
   * @return a Spec with default legal properties suitable for a Stage.
   */
  public static Spec stage() {
    return new Spec(Set.of("name", "class", "conditions", "conditionPolicy"));
  }

  /**
   * Creates a Spec with default legal properties suitable for a Connector. Includes name, class, pipeline, docIdPrefix, and
   * collapse.
   * @return a Spec with default legal properties suitable for a Connector.
   */
  public static Spec connector() {
    return new Spec(Set.of("name", "class", "pipeline", "docIdPrefix", "collapse"));
  }

  /**
   * Creates a ParentSpec with the given name. Has no default legal properties.
   * @param parentName The name of the parent you are creating a spec for. Must not be null.
   * @return A ParentSpec with the given name.
   */
  public static ParentSpec parent(String parentName) {
    if (parentName == null) {
      throw new IllegalArgumentException("ParentName for a Spec must not be null.");
    }

    return new ParentSpec(parentName);
  }

  /**
   * Returns this Spec with the given properties added as required properties.
   * @param properties The required properties you want to add to this Spec.
   * @return This Spec with the given required properties added.
   */
  public Spec withRequiredProperties(String... properties) {
    requiredProperties.addAll(Arrays.asList(properties));
    return this;
  }

  /**
   * Returns this Spec with the given properties added as optional properties.
   * @param properties The optional properties you want to add to this Spec.
   * @return This Spec with the given optional properties added.
   */
  public Spec withOptionalProperties(String... properties) {
    optionalProperties.addAll(Arrays.asList(properties));
    return this;
  }

  /**
   * Returns this Spec with the given parents added as required parents. The ParentSpecs provided will be validated as part of
   * {@link Spec#validate(Config, String)}, and an Exception will be thrown if the parent is missing or has invalid properties.
   * @param properties The required parents you want to add to this Spec.
   * @return This Spec with the given required parents added.
   * @throws IllegalArgumentException If you attempt to add a ParentSpec with a name that is already added to this Spec, either
   * as an optional or required parent.
   */
  public Spec withRequiredParents(ParentSpec... properties) {
    for (ParentSpec parentSpec : properties) {
      if (requiredParentMap.containsKey(parentSpec.parentName) || optionalParentMap.containsKey(parentSpec.parentName)) {
        throw new IllegalArgumentException("There is already a parent with name " + parentSpec.parentName);
      }

      requiredParentMap.put(parentSpec.parentName, parentSpec);
    }

    return this;
  }

  /**
   * Returns this Spec with the given parents added as optional parents. The ParentSpecs provided will be validated as part of
   * {@link Spec#validate(Config, String)}, and an Exception will be thrown if the parent is present and has invalid properties.
   * @param properties The optional parents you want to add to this Spec.
   * @return This Spec with the given ParentSpecs added as optional parents.
   * @throws IllegalArgumentException If you attempt to add a ParentSpec with a name that is already added to this Spec, either
   * as an optional or required parent.
   */
  public Spec withOptionalParents(ParentSpec... properties) {
    for (ParentSpec parentSpec : properties) {
      if (requiredParentMap.containsKey(parentSpec.parentName) || optionalParentMap.containsKey(parentSpec.parentName)) {
        throw new IllegalArgumentException("There is already a parent with name " + parentSpec.parentName);
      }

      optionalParentMap.put(parentSpec.parentName, parentSpec);
    }

    return this;
  }

  /**
   * Returns this Spec with the given parent names added as required parents. No validation will take place on the child properties
   * of the declared required parents in a Config you validate with {@link Spec#validate(Config, String)}. As such, this method
   * should be used for required parents with unpredictable properties / entries.
   * @param optionalParentNames The names of optional parents you want to add to this Spec.
   * @return This Spec with the given ParentSpecs added as optional parents.
   * @throws IllegalArgumentException If you attempt to add an optional parent name that is already a parent name in this Spec, either
   * as an optional or required parent.
   */
  public Spec withRequiredParentNames(String... optionalParentNames) {
    for (String parentName : optionalParentNames) {
      if (requiredParentMap.containsKey(parentName) || optionalParentMap.containsKey(parentName)) {
        throw new IllegalArgumentException("There is already a parent with name " + parentName);
      }

      requiredParentMap.put(parentName, null);
    }

    return this;
  }

  /**
   * Returns this Spec with the given parent names added as optional parents. No validation will take place on the child properties
   * of the declared optional parents, if they are present in a Config you validate with {@link Spec#validate(Config, String)}. As
   * such, this method should be used for optional parents with unpredictable properties / entries.
   * @param optionalParentNames The names of optional parents you want to add to this Spec.
   * @return This Spec with the given ParentSpecs added as optional parents.
   * @throws IllegalArgumentException If you attempt to add an optional parent name that is already a parent name in this Spec, either
   * as an optional or required parent.
   */
  public Spec withOptionalParentNames(String... optionalParentNames) {
    for (String parentName : optionalParentNames) {
      if (requiredParentMap.containsKey(parentName) || optionalParentMap.containsKey(parentName)) {
        throw new IllegalArgumentException("There is already a parent with name " + parentName);
      }

      optionalParentMap.put(parentName, null);
    }

    return this;
  }

  /**
   * Validates this Config using this Spec's properties. Throws an Exception if the Config is missing a required
   * parent / property or contains a non-legal property.
   * @param config The Config that you want to validate against this Spec's properties.
   * @throws IllegalArgumentException If the given config is either missing a required property / parent or contains
   * a non-legal property / parent.
   */
  public void validate(Config config, String displayName) {
    if (!disjoint(requiredProperties, optionalProperties, requiredParentMap.keySet(), optionalParentMap.keySet())) {
      throw new IllegalArgumentException(displayName
          + ": Properties and parents sets must be disjoint.");
    }

    // mainly using a set to prevent repeats with the "unknown parent" message
    Set<String> errorMessages = new HashSet<>();

    // verifies all required properties are present
    Set<String> keys = config.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toSet());
    for (String property : requiredProperties) {
      if (!keys.contains(property)) {
        errorMessages.add("Config must contain property " + property);
      }
    }

    // verifies that
    // 1. all remaining properties are in the optional set or are nested;
    // 2. all required parents are present
    Set<String> observedRequiredParentNames = new HashSet<>();
    Set<String> legalProperties = mergeSets(requiredProperties, optionalProperties, defaultLegalProperties);
    for (String key : keys) {
      if (!legalProperties.contains(key)) {
        String parentName = getParent(key);
        if (parentName == null) {
          errorMessages.add("Config contains unknown property " + key);
        } else if (requiredParentMap.containsKey(parentName)) {
          observedRequiredParentNames.add(parentName);

          Config requiredParentConfig = config.getConfig(parentName);
          ParentSpec requiredParentSpec = requiredParentMap.get(parentName);

          if (requiredParentSpec != null) {
            try {
              requiredParentSpec.validate(requiredParentConfig, parentName);
            } catch (IllegalArgumentException e) {
              errorMessages.add(e.getMessage());
            }
          }
        } else if (optionalParentMap.containsKey(parentName)) {
          Config optionalParentConfig = config.getConfig(parentName);
          ParentSpec optionalParentSpec = optionalParentMap.get(parentName);

          // If not null, then this is a fledged out, optional parent, and the child properties should be validated.
          // if it is null, only its name was declared, and the properties are dynamic / unpredictable.
          if (optionalParentSpec != null) {
            try {
              optionalParentSpec.validate(optionalParentConfig, parentName);
            } catch (IllegalArgumentException e) {
              errorMessages.add(e.getMessage());
            }
          }
        } else {
          errorMessages.add("Config contains unknown parent " + parentName);
        }
      }
    }
    if (observedRequiredParentNames.size() != requiredParentMap.size()) {
      errorMessages.add("Config is missing required parents: " + Sets.difference(requiredParentMap.keySet(), observedRequiredParentNames));
    }

    if (!errorMessages.isEmpty()) {
      throw new IllegalArgumentException("Error(s) with " + displayName + " Config: " + errorMessages);
    }
  }

  /**
   * Returns a Set of the legal properties associated with this Spec.
   * @return a Set of the legal properties associated with this Spec.
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
   * @param displayName A displayName for the object you're validating. Included in any exceptions / error messages.
   * @param requiredProperties The properties you require in your config.
   * @param optionalProperties The properties allowed in your config.
   * @param requiredParents The parents you require in your config.
   * @param optionalParents The parents you allow in your config.
   */
  public static void validateConfig(Config config, String displayName, List<String> requiredProperties, List<String> optionalProperties, List<ParentSpec> requiredParents, List<ParentSpec> optionalParents) {
    Spec spec = new Spec(Set.of())
        .withRequiredProperties(requiredProperties.toArray(new String[0]))
        .withOptionalProperties(optionalProperties.toArray(new String[0]))
        .withRequiredParents(requiredParents.toArray(new ParentSpec[0]))
        .withOptionalParents(optionalParents.toArray(new ParentSpec[0]));

    spec.validate(config, displayName);
  }

  /** Represents a Spec for a Parent in a Config. */
  public static class ParentSpec extends Spec {
    // The name that this parent has in the Config.
    private final String parentName;

    private ParentSpec(String parentName) {
      super(Set.of());
      this.parentName = parentName;
    }

    /* Add an automatic cast to the following methods so we can call them and still have a "ParentSpec" specifically. */
    public ParentSpec withOptionalProperties(String... properties) {
      return (ParentSpec) super.withOptionalProperties(properties);
    }

    public ParentSpec withRequiredProperties(String... properties) {
      return (ParentSpec) super.withRequiredProperties(properties);
    }

    public ParentSpec withRequiredParents(ParentSpec... properties) {
      return (ParentSpec) super.withRequiredParents(properties);
    }

    public ParentSpec withOptionalParents(ParentSpec... properties) {
      return (ParentSpec) super.withOptionalParents(properties);
    }
  }
}
