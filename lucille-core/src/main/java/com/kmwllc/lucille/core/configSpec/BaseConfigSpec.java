package com.kmwllc.lucille.core.configSpec;

import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The base implementation of a ConfigSpec.
 */
public abstract class BaseConfigSpec implements ConfigSpec {

  private final Set<String> requiredProperties;
  private final Set<String> optionalProperties;
  private final Set<String> requiredParents;
  private final Set<String> optionalParents;

  private String displayName;

  /**
   * Creates the base implementation of a ConfigSpec. DisplayName is initially set to "unknown".
   */
  public BaseConfigSpec() {
    requiredProperties = new HashSet<>();
    optionalProperties = new HashSet<>();
    requiredParents = new HashSet<>();
    optionalParents = new HashSet<>();
    this.displayName = "Unknown";
  }

  @Override
  public ConfigSpec withRequiredProperties(String... properties) {
    requiredProperties.addAll(Arrays.asList(properties));
    return this;
  }

  @Override
  public ConfigSpec withOptionalProperties(String... properties) {
    optionalProperties.addAll(Arrays.asList(properties));
    return this;
  }

  @Override
  public ConfigSpec withRequiredParents(String... properties) {
    requiredParents.addAll(Arrays.asList(properties));
    return this;
  }

  @Override
  public ConfigSpec withOptionalParents(String... properties) {
    optionalParents.addAll(Arrays.asList(properties));
    return this;
  }

  @Override
  public void setDisplayName(String newDisplayName) {
    this.displayName = newDisplayName;
  }

  @Override
  public void validate(Config config) {
    if (!ConfigSpecUtils.disjoint(requiredProperties, optionalProperties, requiredParents, optionalParents)) {
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
    Set<String> legalProperties = ConfigSpecUtils.mergeSets(requiredProperties, optionalProperties, getDefaultSpecProperties());
    for (String key : keys) {
      if (!legalProperties.contains(key)) {
        String parent = ConfigSpecUtils.getParent(key);
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

  @Override
  public Set<String> getLegalProperties() {
    return ConfigSpecUtils.mergeSets(requiredProperties, optionalProperties, getDefaultSpecProperties());
  }

  /**
   * Returns the default, allowed properties associated with this specific ConfigSpec implementation.
   * @return the default, allowed properties associated with this specific ConfigSpec implementation.
   */
  protected abstract Set<String> getDefaultSpecProperties();
}
