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

  public static final String CONFIG_PATH = "updateMode";
  public static final UpdateMode DEFAULT = OVERWRITE;

  private String text;

  UpdateMode(String text) {
    this.text = text;
  }

  /**
   * Returns the UpdateMode named by the given String, matched case-insensitively.
   *
   * @throws IllegalArgumentException if the String names no UpdateMode. A misspelled mode used to fall back to OVERWRITE
   * silently, which discarded data; it is now a configuration error.
   */
  public static UpdateMode fromString(String modeStr) {
    for (UpdateMode mode : UpdateMode.values()) {
      if (modeStr != null && modeStr.trim().equalsIgnoreCase(mode.text)) {
        return mode;
      }
    }
    throw new IllegalArgumentException(
        "Unknown " + CONFIG_PATH + " \"" + modeStr + "\"; expected one of append, overwrite, skip");
  }

  public static UpdateMode fromConfig(Config config) {
    if (config.hasPath(CONFIG_PATH)) {
      return UpdateMode.fromString(config.getString(CONFIG_PATH));
    }
    return DEFAULT;
  }

}
