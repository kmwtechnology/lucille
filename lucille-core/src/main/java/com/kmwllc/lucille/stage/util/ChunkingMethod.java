package com.kmwllc.lucille.stage.util;

import com.typesafe.config.Config;

/**
 * A method of chunking contents.
 */
public enum ChunkingMethod {
  /** Chunking by a fixed amount. */
  FIXED("fixed"),
  /** A custom method. */
  CUSTOM("custom"),
  /** Chunking by paragraph. */
  PARAGRAPH("paragraph"),
  /** Chunking by sentence. */
  SENTENCE("sentence");

  /**
   * A path in configuration that should contain a specified chunking method, if desired.
   */
  public static final String CONFIG_PATH = "chunking_method";

  /**
   * The default chunking method, currently "SENTENCE".
   */
  public static final ChunkingMethod DEFAULT = SENTENCE;

  private String text;

  ChunkingMethod(String text) {
    this.text = text;
  }

  /**
   * Returns a chunking method from the given String. If it does not match one of the existing values, it returns the
   * default method. This method is case insensitive.
   *
   * @param modeStr A String representing a chunking method.
   * @return A ChunkingMethod associated with the given String, or the default method if one cannot be extracted.
   */
  public static ChunkingMethod fromString(String modeStr) {
    for (ChunkingMethod mode : ChunkingMethod.values()) {
      if (modeStr.toLowerCase().equals(mode.text)) {
        return mode;
      }
    }

    return DEFAULT;
  }

  /**
   * Gets a chunking method from the given config. Specifically, it reads a String from the "chunking_method" field, and then
   * uses the fromString method on it. If this field is not present, returns the default chunking method.
   *
   * @param config A Configuration that you want to get a ChunkingMethod from.
   * @return A ChunkingMethod extracted from the given Config.
   */
  public static ChunkingMethod fromConfig(Config config) {
    if (config.hasPath(CONFIG_PATH)) {
      return ChunkingMethod.fromString(config.getString(CONFIG_PATH));
    }
    return DEFAULT;
  }

}
