package com.kmwllc.lucille.core.spec;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Specifications for a Config and which properties it is allowed / required to have. Properties are either required
 * or optional, and they have a certain type. The three basic types are number, boolean, and string. You can also
 * specify a List or an Object. For these types, you need to either provide a Spec or a TypeReference describing the
 * list or object.
 *
 * <p> For an object, providing a <code>ParentSpec</code> describes the object's key and the properties it can / must have.
 * Providing a <code>TypeReference</code> describes what the unwrapped ConfigObject should deserialize as / cast to. For example, if you
 * need a "field mapping" that maps Strings to Strings, you should pass in <code>TypeReference&lt;Map&lt;String, String&gt;&gt;</code>.
 *
 * <p> For a list, you should provide a <code>Spec</code> when you need a list of Configs with a specific set of properties. A common
 * example is conditions for a Stage. Provide a <code>TypeReference</code> otherwise, describing what the unwrapped ConfigList should deserialize as
 * / cast to. For example, if you need a List of Doubles, you should pass in <code>TypeReference&lt;List&lt;Double&gt;&gt;</code>.
 *
 * <p> Generally, for objects and lists, use a Spec when you know field names in advance, and a <code>TypeReference</code> when you do not.
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
        new ListProperty("conditions", false, Spec.withoutDefaults()
            .optionalString("operator")
            .optionalList("values", new TypeReference<List<String>>(){})
            .requiredList("fields", new TypeReference<List<String>>(){})),
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

  // ************ Adding Basic Types ****************

  public Spec requiredString(String... requiredStringFieldNames) {
    Arrays.stream(requiredStringFieldNames).forEach(fieldName -> properties.add(new StringProperty(fieldName, true)));
    return this;
  }

  public Spec optionalString(String... optionalStringFieldNames) {
    Arrays.stream(optionalStringFieldNames).forEach(fieldName -> properties.add(new StringProperty(fieldName, false)));
    return this;
  }

  public Spec requiredNumber(String... requiredNumberFieldNames) {
    Arrays.stream(requiredNumberFieldNames).forEach(fieldName -> properties.add(new NumberProperty(fieldName, true)));
    return this;
  }

  public Spec optionalNumber(String... optionalNumberFieldNames) {
    Arrays.stream(optionalNumberFieldNames).forEach(fieldName -> properties.add(new NumberProperty(fieldName, false)));
    return this;
  }

  public Spec requiredBoolean(String... requiredBooleanFieldNames) {
    Arrays.stream(requiredBooleanFieldNames).forEach(fieldName -> properties.add(new BooleanProperty(fieldName, true)));
    return this;
  }

  public Spec optionalBoolean(String... optionalBooleanFieldNames) {
    Arrays.stream(optionalBooleanFieldNames).forEach(fieldName -> properties.add(new BooleanProperty(fieldName, false)));
    return this;
  }

  // ************ Adding Objects (Parents) and Lists ************

  public Spec requiredParent(ParentSpec... requiredParents) {
    Arrays.stream(requiredParents).forEach(requiredParentSpec -> properties.add(new ObjectProperty(requiredParentSpec, true)));
    return this;
  }

  public Spec optionalParent(ParentSpec... optionalParents) {
    Arrays.stream(optionalParents).forEach(optionalParentSpec -> properties.add(new ObjectProperty(optionalParentSpec, false)));
    return this;
  }

  public Spec requiredParent(String name, TypeReference<?> type) {
    properties.add(new ObjectProperty(name, true, type));
    return this;
  }

  public Spec optionalParent(String name, TypeReference<?> type) {
    properties.add(new ObjectProperty(name, false, type));
    return this;
  }

  public Spec requiredList(String name, Spec objectSpec) {
    properties.add(new ListProperty(name, true, objectSpec));
    return this;
  }

  public Spec optionalList(String name, Spec objectSpec) {
    properties.add(new ListProperty(name, false, objectSpec));
    return this;
  }

  public Spec requiredList(String name, TypeReference<?> listType) {
    properties.add(new ListProperty(name, true, listType));
    return this;
  }

  public Spec optionalList(String name, TypeReference<?> listType) {
    properties.add(new ListProperty(name, false, listType));
    return this;
  }

  // ************ Adding Basic Types w/ Descriptions ****************

  public Spec requiredStringWithDescription(String requiredStringFieldName, String description) {
    properties.add(new StringProperty(requiredStringFieldName, true, description));
    return this;
  }

  public Spec optionalStringWithDescription(String optionalStringFieldName, String description) {
    properties.add(new StringProperty(optionalStringFieldName, false, description));
    return this;
  }

  public Spec requiredNumberWithDescription(String requiredNumberFieldName, String description) {
    properties.add(new NumberProperty(requiredNumberFieldName, true, description));
    return this;
  }

  public Spec optionalNumberWithDescription(String optionalNumberFieldName, String description) {
    properties.add(new NumberProperty(optionalNumberFieldName, false, description));
    return this;
  }

  public Spec requiredBooleanWithDescription(String requiredBooleanFieldName, String description) {
    properties.add(new BooleanProperty(requiredBooleanFieldName, true, description));
    return this;
  }

  public Spec optionalBooleanWithDescription(String optionalBooleanFieldName, String description) {
    properties.add(new BooleanProperty(optionalBooleanFieldName, false, description));
    return this;
  }

  // ******** Adding Objects and Lists with Descriptions ********

  public Spec requiredParentWithDescription(ParentSpec requiredParentSpec, String description) {
    properties.add(new ObjectProperty(requiredParentSpec, true, description));
    return this;
  }

  public Spec optionalParentWithDescription(ParentSpec optionalParentSpec, String description) {
    properties.add(new ObjectProperty(optionalParentSpec, false, description));
    return this;
  }

  public Spec requiredParentWithDescription(String requiredParentName, TypeReference<?> type, String description) {
    properties.add(new ObjectProperty(requiredParentName, true, type, description));
    return this;
  }

  public Spec optionalParentWithDescription(String optionalParentName, TypeReference<?> type, String description) {
    properties.add(new ObjectProperty(optionalParentName, false, type, description));
    return this;
  }

  public Spec requiredListWithDescription(String name, Spec objectSpec, String description) {
    properties.add(new ListProperty(name, true, objectSpec, description));
    return this;
  }

  public Spec optionalListWithDescription(String name, Spec objectSpec, String description) {
    properties.add(new ListProperty(name, false, objectSpec, description));
    return this;
  }

  public Spec requiredListWithDescription(String name, TypeReference<?> listType, String description) {
    properties.add(new ListProperty(name, true, listType, description));
    return this;
  }

  public Spec optionalListWithDescription(String name, TypeReference<?> listType, String description) {
    properties.add(new ListProperty(name, false, listType, description));
    return this;
  }

  // ********* Validation / Utility Functions *********

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
    public ParentSpec requiredString(String... requiredStringFieldNames) {
      super.requiredString(requiredStringFieldNames);
      return this;
    }

    @Override
    public ParentSpec optionalString(String... optionalStringFieldNames) {
      super.optionalString(optionalStringFieldNames);
      return this;
    }

    @Override
    public ParentSpec requiredNumber(String... requiredNumberFieldNames) {
      super.requiredNumber(requiredNumberFieldNames);
      return this;
    }

    @Override
    public ParentSpec optionalNumber(String... optionalNumberFieldNames) {
      super.optionalNumber(optionalNumberFieldNames);
      return this;
    }

    @Override
    public ParentSpec requiredBoolean(String... requiredBooleanFieldNames) {
      super.requiredBoolean(requiredBooleanFieldNames);
      return this;
    }

    @Override
    public ParentSpec optionalBoolean(String... optionalBooleanFieldNames) {
      super.optionalBoolean(optionalBooleanFieldNames);
      return this;
    }

    @Override
    public ParentSpec requiredParent(ParentSpec... properties) {
      super.requiredParent(properties);
      return this;
    }

    @Override
    public ParentSpec optionalParent(ParentSpec... properties) {
      super.optionalParent(properties);
      return this;
    }

    @Override
    public ParentSpec requiredParent(String name, TypeReference<?> type) {
      super.requiredParent(name, type);
      return this;
    }

    @Override
    public ParentSpec optionalParent(String name, TypeReference<?> type) {
      super.optionalParent(name, type);
      return this;
    }

    @Override
    public ParentSpec requiredList(String name, Spec objectSpec) {
      super.requiredList(name, objectSpec);
      return this;
    }

    @Override
    public ParentSpec optionalList(String name, Spec objectSpec) {
      super.optionalList(name, objectSpec);
      return this;
    }

    @Override
    public ParentSpec requiredList(String name, TypeReference<?> listType) {
      super.requiredList(name, listType);
      return this;
    }

    @Override
    public ParentSpec optionalList(String name, TypeReference<?> listType) {
      super.optionalList(name, listType);
      return this;
    }

    @Override
    public ParentSpec requiredStringWithDescription(String requiredStringFieldName, String description) {
      super.requiredStringWithDescription(requiredStringFieldName, description);
      return this;
    }

    @Override
    public ParentSpec optionalStringWithDescription(String optionalStringFieldName, String description) {
      super.optionalStringWithDescription(optionalStringFieldName, description);
      return this;
    }

    @Override
    public ParentSpec requiredNumberWithDescription(String requiredNumberFieldName, String description) {
      super.requiredNumberWithDescription(requiredNumberFieldName, description);
      return this;
    }

    @Override
    public ParentSpec optionalNumberWithDescription(String optionalNumberFieldName, String description) {
      super.optionalNumberWithDescription(optionalNumberFieldName, description);
      return this;
    }

    @Override
    public ParentSpec requiredBooleanWithDescription(String requiredBooleanFieldName, String description) {
      super.requiredBooleanWithDescription(requiredBooleanFieldName, description);
      return this;
    }

    @Override
    public ParentSpec optionalBooleanWithDescription(String optionalBooleanFieldName, String description) {
      super.optionalBooleanWithDescription(optionalBooleanFieldName, description);
      return this;
    }

    @Override
    public ParentSpec requiredParentWithDescription(ParentSpec requiredParentSpec, String description) {
      super.requiredParentWithDescription(requiredParentSpec, description);
      return this;
    }

    @Override
    public ParentSpec optionalParentWithDescription(ParentSpec optionalParentSpec, String description) {
      super.optionalParentWithDescription(optionalParentSpec, description);
      return this;
    }

    @Override
    public ParentSpec requiredParentWithDescription(String requiredParentName, TypeReference<?> type, String description) {
      super.requiredParentWithDescription(requiredParentName, type, description);
      return this;
    }

    @Override
    public ParentSpec optionalParentWithDescription(String optionalParentName, TypeReference<?> type, String description) {
      super.optionalParentWithDescription(optionalParentName, type, description);
      return this;
    }

    @Override
    public ParentSpec requiredListWithDescription(String name, Spec objectSpec, String description) {
      super.requiredListWithDescription(name, objectSpec, description);
      return this;
    }

    @Override
    public ParentSpec optionalListWithDescription(String name, Spec objectSpec, String description) {
      super.optionalListWithDescription(name, objectSpec, description);
      return this;
    }

    @Override
    public ParentSpec requiredListWithDescription(String name, TypeReference<?> listType, String description) {
      super.requiredListWithDescription(name, listType, description);
      return this;
    }

    @Override
    public ParentSpec optionalListWithDescription(String name, TypeReference<?> listType, String description) {
      super.optionalListWithDescription(name, listType, description);
      return this;
    }

    public String getParentName() {
      return parentName;
    }
  }
}
