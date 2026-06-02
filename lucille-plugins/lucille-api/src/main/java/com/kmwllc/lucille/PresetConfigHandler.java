package com.kmwllc.lucille;

import com.typesafe.config.Config;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * When configured to do so, handles the retrieval of configs from a specified preset <code>configDirectoryPath</code>.
 */
public class PresetConfigHandler {

  private Path configDirectoryPath;

  public PresetConfigHandler(Path configDirectoryPath) {
    this.configDirectoryPath = configDirectoryPath;
  }

  public Map<String, Config> fetchConfigs() throws IOException {
    Map<String, Config> filenamesToConfigs = new HashMap<>();

    if (configDirectoryPath == null) {
      return filenamesToConfigs;
    }

    try (Stream<Path> files = Files.list(configDirectoryPath)) {
      files.forEach(System.out::println);
    }

    return filenamesToConfigs;
  }
}
