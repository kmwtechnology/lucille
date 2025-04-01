package com.kmwllc.lucille.core.configSpec;

import com.typesafe.config.Config;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Specifications for a Config and which properties it is allowed / required to have.
 */
public interface ConfigSpec {

  /**
   * Returns this ConfigSpec with the given properties added as required properties.
   * @param properties The required properties you want to add to this ConfigSpec.
   * @return This ConfigSpec with the given required properties added.
   */
  ConfigSpec withRequiredProperties(String... properties);

  /**
   * Returns this ConfigSpec with the given properties added as optional properties.
   * @param properties The optional properties you want to add to this ConfigSpec.
   * @return This ConfigSpec with the given optional properties added.
   */
  ConfigSpec withOptionalProperties(String... properties);

  /**
   * Returns this ConfigSpec with the given parents added as required parents.
   * @param properties The required parents you want to add to this ConfigSpec.
   * @return This ConfigSpec with the given required parents added.
   */
  ConfigSpec withRequiredParents(String... properties);

  /**
   * Returns this ConfigSpec with the given parents added as optional parents.
   * @param properties The optional parents you want to add to this ConfigSpec.
   * @return This ConfigSpec with the given optional parents added.
   */
  ConfigSpec withOptionalParents(String... properties);

  /**
   * Sets this ConfigSpec to have the given display name.
   * @param newDisplayName The new display name for this ConfigSpec.
   */
  void setDisplayName(String newDisplayName);

  /**
   * Validates this Config using this ConfigSpec's properties. Throws an Exception if the Config is missing a required
   * parent / property or contains a non-legal property.
   * @param config The Config that you want to validate against this ConfigSpec's properties.
   */
  void validate(Config config);

  /**
   * Returns a Set of the legal properties associated with this ConfigSpec.
   * @return a Set of the legal properties associated with this ConfigSpec.
   */
  Set<String> getLegalProperties();

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
}
