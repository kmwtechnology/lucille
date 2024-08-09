package com.kmwllc.lucille.core;

import com.typesafe.config.Config;

public enum ChunkingMethod {
  FIXED("fixed"), CUSTOM("custom"), PARAGRAPH("paragraph"), SENTENCE("sentence"), SEMANTIC("semantic");

  public static final String CONFIG_PATH = "chunking_method";
  public static final ChunkingMethod DEFAULT = FIXED;

  private String text;

  ChunkingMethod(String text) {
    this.text = text;
  }

  public static ChunkingMethod fromString(String modeStr) {
    for (ChunkingMethod mode : ChunkingMethod.values()) {
      if (modeStr.equals(mode.text)) {
        return mode;
      }
    }

    return DEFAULT;
  }

  public static ChunkingMethod fromConfig(Config config) {
    if (config.hasPath(CONFIG_PATH)) {
      return ChunkingMethod.fromString(config.getString(CONFIG_PATH));
    }
    return DEFAULT;
  }

}
