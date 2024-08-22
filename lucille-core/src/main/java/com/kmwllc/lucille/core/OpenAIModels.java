package com.kmwllc.lucille.core;


import com.typesafe.config.Config;

public enum OpenAIModels {
  TEXT_EMBEDDING_3_SMALL("text-embedding-3-small"), TEXT_EMBEDDING_3_LARGE("text-embedding-3-large"), TEXT_EMBEDDING_ADA_002("text-embedding-ada-002");

  public static final String CONFIG_PATH = "model_name";
  public static final OpenAIModels DEFAULT = TEXT_EMBEDDING_3_SMALL;

  private String modelName;

  OpenAIModels(String text) {
    this.modelName = text;
  }

  public static OpenAIModels fromString(String modelName) {
    for (OpenAIModels mode : OpenAIModels.values()) {
      if (modelName.toLowerCase().equals(mode.modelName)) {
        return mode;
      }
    }
    return DEFAULT;
  }

  public static OpenAIModels fromConfig(Config config) {
    if (config.hasPath(CONFIG_PATH)) {
      return OpenAIModels.fromString(config.getString(CONFIG_PATH));
    }
    return DEFAULT;
  }

  public String getModelName() {
    return modelName;
  }
}
