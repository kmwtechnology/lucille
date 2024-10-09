package com.kmwllc.lucille.stage.util;

import com.typesafe.config.Config;

public enum ChunkingMethod {
  FIXED("fixed"), CUSTOM("custom"), PARAGRAPH("paragraph"), SENTENCE("sentence");

  public static final String CONFIG_PATH = "chunking_method";
  public static final ChunkingMethod DEFAULT = SENTENCE;

  private String text;

  ChunkingMethod(String text) {
    this.text = text;
  }

  public static ChunkingMethod fromString(String modeStr) {
    for (ChunkingMethod mode : ChunkingMethod.values()) {
      if (modeStr.toLowerCase().equals(mode.text)) {
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
