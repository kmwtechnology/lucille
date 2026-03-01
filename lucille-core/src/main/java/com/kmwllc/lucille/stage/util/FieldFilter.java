package com.kmwllc.lucille.stage.util;

import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import java.util.List;

public class FieldFilter {

  private final List<String> whitelist;
  private final  List<String> blacklist;

  public FieldFilter(Config config) throws IllegalArgumentException {
    whitelist = config.hasPath("whitelist") ? config.getStringList("whitelist") : List.of();
    blacklist = config.hasPath("blacklist") ? config.getStringList("blacklist") : List.of();

    if (!whitelist.isEmpty() && !blacklist.isEmpty()) {
      throw new IllegalArgumentException("Provided both a whitelist and blacklist to the stage");
    }
  }

  public boolean isActive() {
    return !whitelist.isEmpty() || !blacklist.isEmpty();
  }

  public boolean shouldInclude(String field) {
    if (!whitelist.isEmpty()) {
      return whitelist.contains(field);
    }
    return !blacklist.contains(field);
  }
}
