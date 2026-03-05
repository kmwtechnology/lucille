package com.kmwllc.lucille.util;

import com.typesafe.config.Config;
import java.util.List;

public class FieldFilter {

  private final List<String> whitelist;
  private final List<String> blacklist;

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
