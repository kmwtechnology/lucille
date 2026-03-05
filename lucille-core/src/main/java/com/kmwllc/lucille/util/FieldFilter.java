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
   * If no naming schematic is passed in but the class still uses a whitelist or blacklist, default name to "whitelist" and
   * "blacklist"
   * @param config
   * @param whitelistKey
   * @param blacklistKey
   */
  public FieldFilter(Config config, String whitelistKey, String blacklistKey) {

    String resolvedWhitelistKey = whitelistKey != null ? whitelistKey : "whitelist";
    String resolvedBlacklistKey = blacklistKey != null ? blacklistKey : "blacklist";

    whitelist = config.hasPath(resolvedWhitelistKey) ? config.getStringList(resolvedWhitelistKey) : List.of();
    blacklist = config.hasPath(resolvedBlacklistKey) ? config.getStringList(resolvedBlacklistKey) : List.of();
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
