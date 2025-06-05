package com.kmwllc.lucille.core.spec;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
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
public class Spec {

  private final Set<Property> properties;

  private Spec(Set<Property> defaultLegalProperties) {
    this.properties = new HashSet<>(defaultLegalProperties);
  }

  // Convenience constructor to create a Spec without default legal properties.
  private Spec() {
    this(Set.of());
  }

  /**
   * Creates a Spec with default legal properties suitable for a Stage. Includes name, class, conditions, and
   * conditionPolicy.
   * @return a Spec with default legal properties suitable for a Stage.
   */
  public static Spec stage() {
    return new Spec(Set.of(
        new StringProperty("name", false),
        new StringProperty("class", false),
        new ListProperty("conditions", false, ConfigValueType.OBJECT),
        new StringProperty("conditionPolicy", false)));
  }

  /**
   * Creates a Spec with default legal properties suitable for a Connector. Includes name, class, pipeline, docIdPrefix, and
   * collapse.
   * @return a Spec with default legal properties suitable for a Connector.
   */
  public static Spec connector() {
    return new Spec(Set.of(
        new StringProperty("name", false),
        new StringProperty("class", false),
        new StringProperty("pipeline", false),
        new StringProperty("docIdPrefix", false),
        new BooleanProperty("collapse", false)));
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
   * @param requiredProperties The required properties you want to add to this Spec.
   * @return This Spec with the given required properties added.
   */
  public Spec withRequiredProperties(String... requiredProperties) {
    Arrays.stream(requiredProperties).forEach(requiredPropertyName -> properties.add(new AnyProperty(requiredPropertyName, true)));
    return this;
  }

  /**
   * Returns this Spec with the given properties added as optional properties.
   * @param optionalProperties The optional properties you want to add to this Spec.
   * @return This Spec with the given optional properties added.
   */
  public Spec withOptionalProperties(String... optionalProperties) {
    Arrays.stream(optionalProperties).forEach(optionalPropertyName -> properties.add(new AnyProperty(optionalPropertyName, false)));
    return this;
  }

  /**
   * Returns this Spec with the given parents added as required parents. The ParentSpecs provided will be validated as part of
   * {@link Spec#validate(Config, String)}, and an Exception will be thrown if the parent is missing or has invalid properties.
   * @param requiredParents The required parents you want to add to this Spec.
   * @return This Spec with the given required parents added.
   * @throws IllegalArgumentException If you attempt to add a ParentSpec with a name that is already added to this Spec, either
   * as an optional or required parent.
   */
  public Spec withRequiredParents(ParentSpec... requiredParents) {
    Arrays.stream(requiredParents).forEach(requiredParentSpec -> properties.add(new ObjectProperty(requiredParentSpec, true)));
    return this;
  }

  /**
   * Returns this Spec with the given parents added as optional parents. The ParentSpecs provided will be validated as part of
   * {@link Spec#validate(Config, String)}, and an Exception will be thrown if the parent is present and has invalid properties.
   * @param optionalParents The optional parents you want to add to this Spec.
   * @return This Spec with the given ParentSpecs added as optional parents.
   * @throws IllegalArgumentException If you attempt to add a ParentSpec with a name that is already added to this Spec, either
   * as an optional or required parent.
   */
  public Spec withOptionalParents(ParentSpec... optionalParents) {
    Arrays.stream(optionalParents).forEach(optionalParentSpec -> properties.add(new ObjectProperty(optionalParentSpec, false)));
    return this;
  }

  /**
   * Returns this Spec with the given parent names added as required parents. No validation will take place on the child properties
   * of the declared required parents in a Config you validate with {@link Spec#validate(Config, String)}. As such, this method
   * should be used for required parents with unpredictable properties / entries.
   * @param requiredParentNames The names of required parents you want to add to this Spec.
   * @return This Spec with the given ParentSpecs added as optional parents.
   * @throws IllegalArgumentException If you attempt to add an optional parent name that is already a parent name in this Spec, either
   * as an optional or required parent.
   */
  public Spec withRequiredParentNames(String... requiredParentNames) {
    Arrays.stream(requiredParentNames).forEach(requiredParentName -> properties.add(new ObjectProperty(requiredParentName, true)));
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
    Arrays.stream(optionalParentNames).forEach(optionalParentName -> properties.add(new ObjectProperty(optionalParentName, false)));
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
    // mainly using a set to prevent repeats with the "unknown parent" message
    Set<String> errorMessages = new HashSet<>();

    Set<String> keys = config.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toSet());

    // This method is responsible for making sure that there are no unknown / illegal field names found.
    // Properties are responsible for making sure a required field is present and for making sure that the
    // fields are mapped to the correct type.
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
      throw new IllegalArgumentException("Error(s) with " + displayName + " Config: " + errorMessages);
    }
  }

  /**
   * Returns a Set of the legal properties associated with this Spec.
   * @return a Set of the legal properties associated with this Spec.
   */
  public Set<String> getLegalProperties() {
    return properties.stream().map(Property::getName).collect(Collectors.toSet());
  }

  // ***** Utility Functions *****

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

  // *****************************

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

    public String getParentName() {
      return parentName;
    }
  }
}
