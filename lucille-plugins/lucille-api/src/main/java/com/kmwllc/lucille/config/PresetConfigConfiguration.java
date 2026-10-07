package com.kmwllc.lucille.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.nio.file.Path;

/**
 * Configuration for declaring a directory containing configurations that should
 * be loaded on initialization of the Lucille API.
 */
public final class PresetConfigConfiguration {

  public PresetConfigConfiguration() { }

  private Path configDirectoryPath;

  /**
   * @return The directory path at which the preset configurations are stored.
   */
  @JsonProperty
  public Path getConfigDirectoryPath() {
    return configDirectoryPath;
  }

  /**
   * Sets the path to the directory that contains the preset configurations to be loaded on initialization of the Lucille API.
   * @param configDirectoryPath A path to the directory containing the configurations to be loaded.
   */
  @JsonProperty
  public void setConfigDirectoryPath(Path configDirectoryPath) {
    this.configDirectoryPath = configDirectoryPath;
  }
}
