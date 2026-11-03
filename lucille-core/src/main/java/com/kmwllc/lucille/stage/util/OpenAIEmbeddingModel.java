package com.kmwllc.lucille.stage.util;


import com.typesafe.config.Config;

public enum OpenAIEmbeddingModel {
  TEXT_EMBEDDING_3_SMALL("text-embedding-3-small"), TEXT_EMBEDDING_3_LARGE("text-embedding-3-large"), TEXT_EMBEDDING_ADA_002("text-embedding-ada-002");

  public static final String CONFIG_PATH = "modelName";
  public static final OpenAIEmbeddingModel DEFAULT = TEXT_EMBEDDING_3_SMALL;

  private final String modelName;

  OpenAIEmbeddingModel(String text) {
    this.modelName = text;
  }

  /**
   * Returns the model named by the given String, matched case-insensitively.
   *
   * @throws IllegalArgumentException if the String names no supported model (a misspelled name used to fall back to
   * the default model silently).
   */
  public static OpenAIEmbeddingModel fromString(String modelName) {
    for (OpenAIEmbeddingModel mode : OpenAIEmbeddingModel.values()) {
      if (modelName != null && modelName.trim().equalsIgnoreCase(mode.modelName)) {
        return mode;
      }
    }
    throw new IllegalArgumentException("Unknown " + CONFIG_PATH + " \"" + modelName
        + "\"; expected one of text-embedding-3-small, text-embedding-3-large, text-embedding-ada-002");
  }

  public static OpenAIEmbeddingModel fromConfig(Config config) {
    if (config.hasPath(CONFIG_PATH)) {
      return OpenAIEmbeddingModel.fromString(config.getString(CONFIG_PATH));
    }
    return DEFAULT;
  }

  public String getModelName() {
    return modelName;
  }
}
