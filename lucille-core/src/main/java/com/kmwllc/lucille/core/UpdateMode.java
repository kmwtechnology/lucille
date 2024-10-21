package com.kmwllc.lucille.core;

import com.typesafe.config.Config;

public enum UpdateMode {
  APPEND("append"), OVERWRITE("overwrite"), SKIP("skip");

  public static final String CONFIG_PATH = "updateMode";
  public static final UpdateMode DEFAULT = OVERWRITE;

  private String text;

  UpdateMode(String text) {
    this.text = text;
  }

  public static UpdateMode fromString(String modeStr) {
    for (UpdateMode mode : UpdateMode.values()) {
      if (modeStr.equals(mode.text)) {
        return mode;
      }
    }

    return DEFAULT;
  }

  public static UpdateMode fromConfig(Config config) {
    if (config.hasPath(CONFIG_PATH)) {
      return UpdateMode.fromString(config.getString(CONFIG_PATH));
    }
    return DEFAULT;
  }

}
