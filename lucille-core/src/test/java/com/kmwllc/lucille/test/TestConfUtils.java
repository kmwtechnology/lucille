package com.kmwllc.lucille.test;

import com.kmwllc.lucille.core.Runner;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * This class is only meant for testing configs in the Lucille example module.
 */

public class TestConfUtils {

  public static void validateConfigs(List<String> configsToIgnore) throws Exception {
    // get path to conf directory
    Path currentDir = Paths.get("").toAbsolutePath();
    Path confDir = currentDir.resolve("conf");

    try (Stream<Path> confStream = Files.list(confDir)) {
      // retrieve all conf files in conf folder
      List<Path> confPaths = confStream.filter(p -> p.toString().endsWith(".conf")).toList();

      // validate each of the conf files
      for (Path configPath : confPaths) {
        if (configsToIgnore.contains(configPath.getFileName().toString())) {
          continue;
        }
        // run the config and gather exceptions
        Config config = ConfigFactory.parseFile(configPath.toFile());
        Map<String, List<Exception>> exceptions = Runner.runInValidationMode(config);

        for (Map.Entry<String, List<Exception>> entry : exceptions.entrySet()) {
          if (!entry.getValue().isEmpty()) {
            throw new Exception("configuration validation failed for " + entry.getKey() + ": " + entry.getValue());
          }
        }
      }
    }
  }

  public static void validateConfigs() throws Exception {
    validateConfigs(new ArrayList<>());
  }

}
