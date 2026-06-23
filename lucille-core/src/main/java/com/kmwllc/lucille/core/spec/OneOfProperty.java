package com.kmwllc.lucille.core.spec;

import com.fasterxml.jackson.databind.JsonNode;
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
 * A property that wraps multiple Specs and enforces that exactly one of them is both present and
 * valid in the config.
 *
 * <p> "Present" means all of that spec's required properties exist in the config, as determined by
 * {@link Spec#requiredPropertiesPresent(Config)}. Exactly one spec must be present; zero or more
 * than one both result in an error.
 *
 * <p> Keys that belong to a non-matched option are flagged as invalid. Keys that belong to no
 * option at all (e.g. sibling properties in a parent spec) are left to the outer {@link Spec}'s
 * unknown-key check, which sees the union of all options' names via {@link #getLegalPropertyNames()}.
 */
public class OneOfProperty extends Property {

  private final List<Spec> specs;

  public OneOfProperty(Spec... specs) {
    this.specs = Arrays.asList(specs);
  }

  @Override
  public void validate(Config config) throws IllegalArgumentException {
    List<Spec> matching = specs.stream()
        .filter(s -> s.requiredPropertiesPresent(config))
        .toList();

    if (matching.isEmpty()) {
      throw new IllegalArgumentException(
          "Exactly one of the following options must be specified, but none were found: " + describeOptions());
    }
    if (matching.size() > 1) {
      throw new IllegalArgumentException(
          "Exactly one of the following options must be specified, but multiple were found: " + describeOptions());
    }

    // Validate each property in the matched spec directly. We don't call spec.validate()
    // because that would flag keys from other options as unknown.
    Set<String> errorMessages = new HashSet<>();
    for (Property p : matching.get(0).getProperties()) {
      try {
        p.validate(config);
      } catch (IllegalArgumentException e) {
        errorMessages.add(e.getMessage());
      }
    }

    // Special handling to ensure that *optional* keys belonging to another "choice" that is not used
    // are not present in the config. we throw a warning since that could indicate some mix
    // between the distinct options available.
    Set<String> matchedLegalNames = matching.get(0).getLegalProperties();
    Set<String> allOneOfNames = getLegalPropertyNames();
    for (String key : config.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toSet())) {
      String effectiveKey = Spec.getParent(key) != null ? Spec.getParent(key) : key;
      if (allOneOfNames.contains(effectiveKey) && !matchedLegalNames.contains(effectiveKey)) {
        errorMessages.add("Property \"" + effectiveKey + "\" is not valid for the matched oneOf option");
      }
    }

    if (!errorMessages.isEmpty()) {
      if (errorMessages.size() == 1) {
        throw new IllegalArgumentException(errorMessages.iterator().next());
      } else {
        throw new IllegalArgumentException("Errors in oneOf option: " + errorMessages);
      }
    }
  }

  @Override
  public boolean requiredPresent(Config config) {
    return specs.stream().filter(s -> s.requiredPropertiesPresent(config)).count() == 1;
  }

  @Override
  public Set<String> getLegalPropertyNames() {
    return specs.stream()
        .flatMap(s -> s.getLegalProperties().stream())
        .collect(Collectors.toSet());
  }

  @Override
  public JsonNode json() {
    ObjectNode node = MAPPER.createObjectNode();
    node.put("type", "ONE_OF");

    ArrayNode specsArray = MAPPER.createArrayNode();
    for (Spec spec : specs) {
      specsArray.add(spec.toJson());
    }
    node.set("specs", specsArray);

    return node;
  }

  private String describeOptions() {
    return specs.stream()
        .map(s -> s.getLegalProperties().toString())
        .collect(Collectors.joining(" OR "));
  }
}
