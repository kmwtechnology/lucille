package com.kmwllc.lucille.core;

import com.typesafe.config.Config;

/**
 * The methodology by which a field in a Document will be modified.
 *
 * <p>APPEND: the provided values will be appended to the field.
 * <p>OVERWRITE: the provided values will overwrite any current field values.
 * <p>SKIP: the provided values will populate the field if the field didn't previously exist; otherwise no change will be made.
 */
public enum UpdateMode {
  APPEND("append"), OVERWRITE("overwrite"), SKIP("skip");

  public static final String CONFIG_PATH = "update_mode";
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
