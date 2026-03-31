package com.kmwllc.lucille.util;

import com.typesafe.config.Config;
import java.util.List;

/**
 * Standardizes whitelist and blacklist implementations for document fields. Applicable to any class which processes documents.
 */
public class FieldFilter {

  private final List<String> whitelist;
  private final List<String> blacklist;

  /**
   * Creates an inactive FieldFilter with empty whitelist and blacklist.
   */
  public FieldFilter() {
    this.whitelist = List.of();
    this.blacklist = List.of();
  }

  /**
   * If a whitelist or blacklist is not passed in, default to an empty list.
   * @param config the config for Lucille
   */
  public FieldFilter(Config config) {
    this.whitelist = config.hasPath("whitelist") ? List.copyOf(config.getStringList("whitelist")) : List.of();
    this.blacklist = config.hasPath("blacklist") ? List.copyOf(config.getStringList("blacklist")) : List.of();
  }

  public List<String> getWhitelist() {
    return whitelist;
  }

  public List<String> getBlacklist() {
    return blacklist;
  }

  public boolean isActive() {
    return !whitelist.isEmpty() || !blacklist.isEmpty();
  }

  public boolean shouldInclude(String field) {
    if (!whitelist.isEmpty() && !blacklist.isEmpty()) {
      return whitelist.contains(field) && !blacklist.contains(field);
    } else if (!whitelist.isEmpty()) {
      return whitelist.contains(field);
    }
    return !blacklist.contains(field);
  }
}
