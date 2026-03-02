package com.kmwllc.lucille.stage.util;

import com.typesafe.config.Config;
import dev.langchain4j.agent.tool.P;
import java.util.List;

public class FieldFilter {

  private final List<String> whitelist;
  private final List<String> blacklist;

  public FieldFilter(Config config, String whitelistKey, String blacklistKey) {

    if (whitelistKey == null) {
      whitelistKey = "whitelist";
    }
    if (blacklistKey == null) {
      blacklistKey = "blacklist";
    }

    whitelist = config.hasPath(whitelistKey) ? config.getStringList(whitelistKey) : List.of();
    blacklist = config.hasPath(blacklistKey) ? config.getStringList(blacklistKey) : List.of();
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
