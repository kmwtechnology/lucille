package com.kmwllc.lucille.core;

import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Specifications for a Config and which properties it is allowed / required to have.
 */
public class Spec {

  private final Set<String> defaultLegalProperties;

  private final Set<String> requiredProperties;
  private final Set<String> optionalProperties;

  // Mapping parent names to the corresponding ParentSpecs. withRequired/OptionalParentNames will add those names to the map,
  // mapping them to null. Methods throw exceptions if a parent name is declared more than once (among both required and
  // optional).
  private final Map<String, ParentSpec> requiredParentMap;
  private final Map<String, ParentSpec> optionalParentMap;

  private Spec(Set<String> defaultLegalProperties) {
    this.defaultLegalProperties = defaultLegalProperties;

    this.requiredProperties = new HashSet<>();
    this.optionalProperties = new HashSet<>();

    this.requiredParentMap = new HashMap<>();
    this.optionalParentMap = new HashMap<>();
  }

  private Spec() {
    this(Set.of());
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
   * Creates a Spec for a specific Indexer implementation (elasticsearch, solr, csv, etc.) There are no common, legal properties
   * associated with specific Indexers.
   * <p> <b>Note:</b> This spec should <b>not</b> be used to validate the general "indexer" block of a Lucille configuration.
   * <p> <b>Note:</b> You should define your required/optional properties/parents <i>without</i> including your indexer-specific
   * parent name / key. For example, do <i>not</i> write "elasticsearch.index"; instead, write "index".
   * @return a Spec with default legal properties suitable for a specific Indexer implementation.
   */
  public static Spec indexer() { return new Spec(); }

  /**
   * Creates a Spec with default legal properties suitable for a FileHandler. Includes "class" and "docIdPrefix".
   *
   * @return a Spec with default legal properties suitable for a FileHandler.
   */
  public static Spec fileHandler() { return new Spec(Set.of("class", "docIdPrefix")); }

  /**
   * Creates a Spec without any default, legal properties.
   * @return a Spec without any default, legal properties.
   */
  public static Spec withoutDefaults() { return new Spec(); }

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
   *
   * <p> <b>Note:</b> If you have a ParentSpec with a required property, an Exception will only be thrown during validation
   * if the parent is present in a Config but that required property is missing.
   *
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

    // Handling of a small edge case - when a parent is just a "parentName", but the contents inside are all empty.
    // (See ApplyFileHandlers - "jsonOnly.conf" as an example - handlerOptions is a requiredParent, and it contains "json: {}"
    // to use it as a file handler, but no inner configuration for json. So it doesn't show up in "keys" above. We also
    // can't just use config.root().keySet().)
    for (Entry<String, ParentSpec> requiredEntry : requiredParentMap.entrySet()) {
      // has no ParentSpec, and is present in the config. might be empty, so not part of "keys" above.
      if (requiredEntry.getValue() == null && config.hasPath(requiredEntry.getKey())) {
        observedRequiredParentNames.add(requiredEntry.getKey());
      }
    }

    if (observedRequiredParentNames.size() != requiredParentMap.size()) {
      errorMessages.add("Config is missing required parents: " + Sets.difference(requiredParentMap.keySet(), observedRequiredParentNames));
    }

    if (!errorMessages.isEmpty()) {
      if (errorMessages.size() == 1) {
        // Saying only "error", and not wrapping the list in [] brackets
        throw new IllegalArgumentException("Error with " + displayName + " Config: " + errorMessages.iterator().next());
      } else {
        throw new IllegalArgumentException("Errors with " + displayName + " Config: " + errorMessages);
      }
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
      super();
      this.parentName = parentName;
    }

    /* Override the following methods - call the super methods, which are still mutating, but return this (a ParentSpec) instead of a Spec.
    Allows us to define static ParentSpecs in various classes (OpensearchUtils, SolrUtils) that can be used in withOptional/RequiredParents. */
    @Override
    public ParentSpec withOptionalProperties(String... properties) {
      super.withOptionalProperties(properties);
      return this;
    }

    @Override
    public ParentSpec withRequiredProperties(String... properties) {
      super.withRequiredProperties(properties);
      return this;
    }

    @Override
    public ParentSpec withRequiredParents(ParentSpec... properties) {
      super.withRequiredParents(properties);
      return this;
    }

    @Override
    public ParentSpec withOptionalParents(ParentSpec... properties) {
      super.withOptionalParents(properties);
      return this;
    }

    @Override
    public ParentSpec withRequiredParentNames(String... properties) {
      super.withRequiredParentNames(properties);
      return this;
    }

    @Override
    public ParentSpec withOptionalParentNames(String... properties) {
      super.withOptionalParentNames(properties);
      return this;
    }
  }
}
