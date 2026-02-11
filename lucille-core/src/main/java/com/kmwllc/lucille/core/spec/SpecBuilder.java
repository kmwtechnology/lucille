package com.kmwllc.lucille.core.spec;

import com.fasterxml.jackson.core.type.TypeReference;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SpecBuilder {

  private final Set<Property> properties;
  private final String name;

  SpecBuilder(String name) {
    this.name = name;
    this.properties = new HashSet();
  }

  SpecBuilder(Set<Property> defaultLegalProperties) {
    this.name = null;
    this.properties = new HashSet<>(defaultLegalProperties);
  }

  // Convenience constructor to create a Spec without default legal properties.
  SpecBuilder() {
    this(Set.of());
  }

  // ********** "Constructors" **********

  /**
   * Creates a Spec with default legal properties suitable for a Stage. Includes name, class, conditions, and
   * conditionPolicy.
   * @return a Spec with default legal properties suitable for a Stage.
   */
  public static SpecBuilder stage() {
    return new SpecBuilder(Set.of(
        new StringProperty("name", false),
        new StringProperty("class", false),
        new ListProperty("conditions", false, SpecBuilder.withoutDefaults()
            .optionalString("operator")
            .optionalString("valuesPath")
            .optionalList("values", new TypeReference<List<String>>(){})
            .requiredList("fields", new TypeReference<List<String>>(){}).build()),
        new StringProperty("conditionPolicy", false)));
  }

  /**
   * Creates a Spec with default legal properties suitable for a Connector. Includes name, class, pipeline, docIdPrefix, and
   * collapse.
   * @return a Spec with default legal properties suitable for a Connector.
   */
  public static SpecBuilder connector() {
    return new SpecBuilder(Set.of(
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
  public static SpecBuilder indexer() { return new SpecBuilder(); }

  /**
   * Creates a Spec with default legal properties suitable for a FileHandler. Includes "class" and "docIdPrefix".
   *
   * @return a Spec with default legal properties suitable for a FileHandler.
   */
  public static SpecBuilder fileHandler() {
    return new SpecBuilder(Set.of(
        new StringProperty("class", false),
        new StringProperty("docIdPrefix", false)));
  }

  /**
   * Creates a Spec without any default, legal properties.
   * @return a Spec without any default, legal properties.
   */
  public static SpecBuilder withoutDefaults() { return new SpecBuilder(); }

  /**
   * Creates a ParentSpec with the given name. Has no default legal properties.
   * @param parentName The name of the parent you are creating a spec for. Must not be null.
   * @return A ParentSpec with the given name.
   */
  public static SpecBuilder parent(String parentName) {
    if (parentName == null) {
      throw new IllegalArgumentException("ParentName for a Spec must not be null.");
    }

    return new SpecBuilder(parentName);
  }

  // ************* Basic Properties **************

  /**
   * Returns this Spec with the given properties added as required properties.
   * @param requiredProperties The required properties you want to add to this Spec.
   * @return This Spec with the given required properties added.
   */
  public SpecBuilder withRequiredProperties(String... requiredProperties) {
    Arrays.stream(requiredProperties).forEach(requiredPropertyName -> properties.add(new AnyProperty(requiredPropertyName, true)));
    return this;
  }

  /**
   * Returns this Spec with the given properties added as optional properties.
   * @param optionalProperties The optional properties you want to add to this Spec.
   * @return This Spec with the given optional properties added.
   */
  public SpecBuilder withOptionalProperties(String... optionalProperties) {
    Arrays.stream(optionalProperties).forEach(optionalPropertyName -> properties.add(new AnyProperty(optionalPropertyName, false)));
    return this;
  }

  // ************ Adding Basic Types ****************

  public SpecBuilder requiredString(String... requiredStringFieldNames) {
    Arrays.stream(requiredStringFieldNames).forEach(fieldName -> properties.add(new StringProperty(fieldName, true)));
    return this;
  }

  public SpecBuilder optionalString(String... optionalStringFieldNames) {
    Arrays.stream(optionalStringFieldNames).forEach(fieldName -> properties.add(new StringProperty(fieldName, false)));
    return this;
  }

  public SpecBuilder requiredNumber(String... requiredNumberFieldNames) {
    Arrays.stream(requiredNumberFieldNames).forEach(fieldName -> properties.add(new NumberProperty(fieldName, true)));
    return this;
  }

  public SpecBuilder optionalNumber(String... optionalNumberFieldNames) {
    Arrays.stream(optionalNumberFieldNames).forEach(fieldName -> properties.add(new NumberProperty(fieldName, false)));
    return this;
  }

  public SpecBuilder requiredBoolean(String... requiredBooleanFieldNames) {
    Arrays.stream(requiredBooleanFieldNames).forEach(fieldName -> properties.add(new BooleanProperty(fieldName, true)));
    return this;
  }

  public SpecBuilder optionalBoolean(String... optionalBooleanFieldNames) {
    Arrays.stream(optionalBooleanFieldNames).forEach(fieldName -> properties.add(new BooleanProperty(fieldName, false)));
    return this;
  }

  // ************ Adding Objects (Parents) and Lists ************

  public SpecBuilder requiredParent(Spec... requiredParents) {
    // TODO: assert that all specs are isParent()
    Arrays.stream(requiredParents).forEach(requiredParentSpec -> properties.add(new ObjectProperty(requiredParentSpec, true)));
    return this;
  }

  public SpecBuilder optionalParent(Spec... optionalParents) {
    // TODO: assert that all specs are isParent()
    Arrays.stream(optionalParents).forEach(optionalParentSpec -> properties.add(new ObjectProperty(optionalParentSpec, false)));
    return this;
  }

  public SpecBuilder requiredParent(String name, TypeReference<?> type) {
    properties.add(new ObjectProperty(name, true, type));
    return this;
  }

  public SpecBuilder optionalParent(String name, TypeReference<?> type) {
    properties.add(new ObjectProperty(name, false, type));
    return this;
  }

  public SpecBuilder requiredList(String name, Spec objectSpec) {
    properties.add(new ListProperty(name, true, objectSpec));
    return this;
  }

  public SpecBuilder optionalList(String name, Spec objectSpec) {
    properties.add(new ListProperty(name, false, objectSpec));
    return this;
  }

  public SpecBuilder requiredList(String name, TypeReference<?> listType) {
    properties.add(new ListProperty(name, true, listType));
    return this;
  }

  public SpecBuilder optionalList(String name, TypeReference<?> listType) {
    properties.add(new ListProperty(name, false, listType));
    return this;
  }

  // ************ Adding Basic Types w/ Descriptions ****************

  public SpecBuilder requiredStringWithDescription(String requiredStringFieldName, String description) {
    properties.add(new StringProperty(requiredStringFieldName, true, description));
    return this;
  }

  public SpecBuilder optionalStringWithDescription(String optionalStringFieldName, String description) {
    properties.add(new StringProperty(optionalStringFieldName, false, description));
    return this;
  }

  public SpecBuilder requiredNumberWithDescription(String requiredNumberFieldName, String description) {
    properties.add(new NumberProperty(requiredNumberFieldName, true, description));
    return this;
  }

  public SpecBuilder optionalNumberWithDescription(String optionalNumberFieldName, String description) {
    properties.add(new NumberProperty(optionalNumberFieldName, false, description));
    return this;
  }

  public SpecBuilder requiredBooleanWithDescription(String requiredBooleanFieldName, String description) {
    properties.add(new BooleanProperty(requiredBooleanFieldName, true, description));
    return this;
  }

  public SpecBuilder optionalBooleanWithDescription(String optionalBooleanFieldName, String description) {
    properties.add(new BooleanProperty(optionalBooleanFieldName, false, description));
    return this;
  }

  // ******** Adding Objects and Lists with Descriptions ********

  public SpecBuilder requiredParentWithDescription(Spec requiredParentSpec, String description) {
    // TODO: assert that all specs are isParent()
    properties.add(new ObjectProperty(requiredParentSpec, true, description));
    return this;
  }

  public SpecBuilder optionalParentWithDescription(Spec optionalParentSpec, String description) {
    // TODO: assert that all specs are isParent()
    properties.add(new ObjectProperty(optionalParentSpec, false, description));
    return this;
  }

  public SpecBuilder requiredParentWithDescription(String requiredParentName, TypeReference<?> type, String description) {
    properties.add(new ObjectProperty(requiredParentName, true, type, description));
    return this;
  }

  public SpecBuilder optionalParentWithDescription(String optionalParentName, TypeReference<?> type, String description) {
    properties.add(new ObjectProperty(optionalParentName, false, type, description));
    return this;
  }

  public SpecBuilder requiredListWithDescription(String name, Spec objectSpec, String description) {
    properties.add(new ListProperty(name, true, objectSpec, description));
    return this;
  }

  public SpecBuilder optionalListWithDescription(String name, Spec objectSpec, String description) {
    properties.add(new ListProperty(name, false, objectSpec, description));
    return this;
  }

  public SpecBuilder requiredListWithDescription(String name, TypeReference<?> listType, String description) {
    properties.add(new ListProperty(name, true, listType, description));
    return this;
  }

  public SpecBuilder optionalListWithDescription(String name, TypeReference<?> listType, String description) {
    properties.add(new ListProperty(name, false, listType, description));
    return this;
  }

 // ******** Adding a Spec ********

  /**
   * Returns this SpecBuilder with all the properties of the provided Spec added
   * @param spec The spec you want to include.
   * @return A SpecBuilder with the given spec added.
   */
  public SpecBuilder include(Spec spec) {
    this.properties.addAll(spec.getProperties());
    return this;
  }

  public Spec build() {
    return new Spec(name, properties);
  }
}
