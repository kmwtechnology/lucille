package com.kmwllc.lucille.core.configSpec;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Utility methods for a StageSpec (namely its validation of a Config)
 */
public class ConfigSpecUtils {

  /**
   * Returns whether the provided sets are disjoint. You must specify at least one set.
   *
   * @param sets The sets you want to check.
   * @return Whether the provided sets are disjoint.
   */
  @SafeVarargs
  public static boolean disjoint(Set<String>... sets) {
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
  public static <T> Set<T> mergeSets(Set<T>... sets) {
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
  public static String getParent(String property) {
    int dotIndex = property.indexOf('.');
    if (dotIndex < 0 || dotIndex == property.length() - 1) {
      return null;
    }
    return property.substring(0, dotIndex);
  }
}
