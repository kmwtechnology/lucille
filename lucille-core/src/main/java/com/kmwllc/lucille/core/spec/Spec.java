package com.kmwllc.lucille.core.spec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Specifications for a Config and which properties it is allowed / required to have.
 */
public class Spec {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final Set<Property> properties;

  private Spec(Set<Property> defaultLegalProperties) {
    this.properties = new HashSet<>(defaultLegalProperties);
  }

  // Convenience constructor to create a Spec without default legal properties.
  private Spec() {
    this(Set.of());
  }

  // ********** "Constructors" **********

  /**
   * Creates a Spec with default legal properties suitable for a Stage. Includes name, class, conditions, and
   * conditionPolicy.
   * @return a Spec with default legal properties suitable for a Stage.
   */
  public static Spec stage() {
    return new Spec(Set.of(
        new StringProperty("name", false),
        new StringProperty("class", false),
        // the conditions get individually validated in Stage.
        // TODO: The UI might not be able to access that information though.
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

  // ***********************************


  // ************* Basic Properties **************

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
  public Spec reqParent(ParentSpec... requiredParents) {
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
  public Spec optParent(ParentSpec... optionalParents) {
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
  public Spec reqParentName(String... requiredParentNames) {
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
  public Spec optParentName(String... optionalParentNames) {
    Arrays.stream(optionalParentNames).forEach(optionalParentName -> properties.add(new ObjectProperty(optionalParentName, false)));
    return this;
  }

  // ************ Adding Basic Types ****************

  public Spec reqStr(String... requiredStringFieldNames) {
    Arrays.stream(requiredStringFieldNames).forEach(fieldName -> properties.add(new StringProperty(fieldName, true)));
    return this;
  }

  public Spec optStr(String... optionalStringFieldNames) {
    Arrays.stream(optionalStringFieldNames).forEach(fieldName -> properties.add(new StringProperty(fieldName, false)));
    return this;
  }

  public Spec reqNum(String... requiredNumberFieldNames) {
    Arrays.stream(requiredNumberFieldNames).forEach(fieldName -> properties.add(new NumberProperty(fieldName, true)));
    return this;
  }

  public Spec optNum(String... optionalNumberFieldNames) {
    Arrays.stream(optionalNumberFieldNames).forEach(fieldName -> properties.add(new NumberProperty(fieldName, false)));
    return this;
  }

  public Spec reqBool(String... requiredBooleanFieldNames) {
    Arrays.stream(requiredBooleanFieldNames).forEach(fieldName -> properties.add(new BooleanProperty(fieldName, true)));
    return this;
  }

  public Spec optBool(String... optionalBooleanFieldNames) {
    Arrays.stream(optionalBooleanFieldNames).forEach(fieldName -> properties.add(new BooleanProperty(fieldName, false)));
    return this;
  }

  public Spec reqList(String... requiredListFieldNames) {
    Arrays.stream(requiredListFieldNames).forEach(fieldName -> properties.add(new ListProperty(fieldName, true, null)));
    return this;
  }

  public Spec optList(String... optionalListFieldNames) {
    Arrays.stream(optionalListFieldNames).forEach(fieldName -> properties.add(new ListProperty(fieldName, false, null)));
    return this;
  }

  // ******************************************

  // ************ Adding Basic Types w/ Descriptions ****************

  public Spec reqStrWithDesc(String requiredStringFieldName, String description) {
    properties.add(new StringProperty(requiredStringFieldName, true, description));
    return this;
  }

  public Spec optStrWithDesc(String optionalStringFieldName, String description) {
    properties.add(new StringProperty(optionalStringFieldName, false, description));
    return this;
  }

  public Spec reqNumWithDesc(String requiredNumberFieldName, String description) {
    properties.add(new NumberProperty(requiredNumberFieldName, true, description));
    return this;
  }

  public Spec optNumWithDesc(String optionalNumberFieldName, String description) {
    properties.add(new NumberProperty(optionalNumberFieldName, false, description));
    return this;
  }

  public Spec reqBoolWithDesc(String requiredBooleanFieldName, String description) {
    properties.add(new BooleanProperty(requiredBooleanFieldName, true, description));
    return this;
  }

  public Spec optBoolWithDesc(String optionalBooleanFieldName, String description) {
    properties.add(new BooleanProperty(optionalBooleanFieldName, false, description));
    return this;
  }

  public Spec reqListWithDesc(String requiredListFieldName, String description) {
    properties.add(new ListProperty(requiredListFieldName, true, null, description));
    return this;
  }

  public Spec optListWithDesc(String optionalBooleanFieldName, String description) {
    properties.add(new ListProperty(optionalBooleanFieldName, false, null, description));
    return this;
  }

  // **********************************************************


  // ******** Adding Objects w/ Descriptions ********

  public Spec reqParentWithDesc(ParentSpec requiredParentSpec, String description) {
    properties.add(new ObjectProperty(requiredParentSpec, true, description));
    return this;
  }

  public Spec optParentWithDesc(ParentSpec optionalParentSpec, String description) {
    properties.add(new ObjectProperty(optionalParentSpec, false, description));
    return this;
  }

  public Spec reqParentNameWithDesc(String requiredParentName, String description) {
    properties.add(new ObjectProperty(requiredParentName, true, description));
    return this;
  }

  public Spec optParentNameWithDesc(String optionalParentName, String description) {
    properties.add(new ObjectProperty(optionalParentName, false, description));
    return this;
  }

  // ************************************************


  // ********* Validation / Utility Functions *********

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
      throw new IllegalArgumentException("Error(s) with " + displayName + " Config: " + errorMessages);
    }
  }

  /**
   * Returns a Set of the legal properties associated with this Spec, include required/optional properties/parents
   * and any default properties associated with the Spec as well.
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
        .reqParent(requiredParents.toArray(new ParentSpec[0]))
        .optParent(optionalParents.toArray(new ParentSpec[0]));

    spec.validate(config, displayName);
  }

  /**
   * Serializes this Spec into a JsonNode describing the properties it declares. The returned node will contain one entry, <code>fields</code>,
   * which will be a list of JsonNode(s) describing the associated properties. See {@link Property#json()} for information on what these
   * objects will contain.
   * @return A JsonNode describing this Spec and the properties it declares.
   */
  public JsonNode serialize() {
    ObjectNode node = MAPPER.createObjectNode();

    ArrayNode fieldsArray = MAPPER.createArrayNode();

    for (Property prop : properties) {
      fieldsArray.add(prop.json());
    }

    node.set("fields", fieldsArray);

    return node;
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
    public ParentSpec reqParent(ParentSpec... properties) {
      super.reqParent(properties);
      return this;
    }

    @Override
    public ParentSpec optParent(ParentSpec... properties) {
      super.optParent(properties);
      return this;
    }

    @Override
    public ParentSpec reqParentName(String... properties) {
      super.reqParentName(properties);
      return this;
    }

    @Override
    public ParentSpec optParentName(String... properties) {
      super.optParentName(properties);
      return this;
    }

    @Override
    public ParentSpec reqStr(String... requiredStringFieldNames) {
      super.reqStr(requiredStringFieldNames);
      return this;
    }

    @Override
    public ParentSpec optStr(String... optionalStringFieldNames) {
      super.optStr(optionalStringFieldNames);
      return this;
    }

    @Override
    public ParentSpec reqNum(String... requiredNumberFieldNames) {
      super.reqNum(requiredNumberFieldNames);
      return this;
    }

    @Override
    public ParentSpec optNum(String... optionalNumberFieldNames) {
      super.optNum(optionalNumberFieldNames);
      return this;
    }

    @Override
    public ParentSpec reqBool(String... requiredBooleanFieldNames) {
      super.reqBool(requiredBooleanFieldNames);
      return this;
    }

    @Override
    public ParentSpec optBool(String... optionalBooleanFieldNames) {
      super.optBool(optionalBooleanFieldNames);
      return this;
    }

    @Override
    public ParentSpec reqList(String... requiredListFieldNames) {
      super.reqList(requiredListFieldNames);
      return this;
    }

    @Override
    public ParentSpec optList(String... optionalListFieldNames) {
      super.optList(optionalListFieldNames);
      return this;
    }

    @Override
    public ParentSpec reqStrWithDesc(String requiredStringFieldName, String description) {
      super.reqStrWithDesc(requiredStringFieldName, description);
      return this;
    }

    @Override
    public ParentSpec optStrWithDesc(String optionalStringFieldName, String description) {
      super.optStrWithDesc(optionalStringFieldName, description);
      return this;
    }

    @Override
    public ParentSpec reqNumWithDesc(String requiredNumberFieldName, String description) {
      super.reqNumWithDesc(requiredNumberFieldName, description);
      return this;
    }

    @Override
    public ParentSpec optNumWithDesc(String optionalNumberFieldName, String description) {
      super.optNumWithDesc(optionalNumberFieldName, description);
      return this;
    }

    @Override
    public ParentSpec reqBoolWithDesc(String requiredBooleanFieldName, String description) {
      super.reqBoolWithDesc(requiredBooleanFieldName, description);
      return this;
    }

    @Override
    public ParentSpec optBoolWithDesc(String optionalBooleanFieldName, String description) {
      super.optBoolWithDesc(optionalBooleanFieldName, description);
      return this;
    }

    @Override
    public ParentSpec reqListWithDesc(String requiredListFieldName, String description) {
      super.reqListWithDesc(requiredListFieldName, description);
      return this;
    }

    @Override
    public ParentSpec optListWithDesc(String optionalListFieldName, String description) {
      super.optListWithDesc(optionalListFieldName, description);
      return this;
    }

    @Override
    public ParentSpec reqParentWithDesc(ParentSpec requiredParentSpec, String description) {
      super.reqParentWithDesc(requiredParentSpec, description);
      return this;
    }

    @Override
    public ParentSpec optParentWithDesc(ParentSpec optionalParentSpec, String description) {
      super.optParentWithDesc(optionalParentSpec, description);
      return this;
    }

    @Override
    public ParentSpec reqParentNameWithDesc(String requiredParentName, String description) {
      super.reqParentNameWithDesc(requiredParentName, description);
      return this;
    }

    @Override
    public ParentSpec optParentNameWithDesc(String optionalParentName, String description) {
      super.optParentNameWithDesc(optionalParentName, description);
      return this;
    }

    public String getParentName() {
      return parentName;
    }
  }
}
