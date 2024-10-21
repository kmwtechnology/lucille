package com.kmwllc.lucille.opensearchIngest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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
import org.junit.Before;
import org.junit.Test;

public class TestConfigs {
  List<String> configsToIgnore;

  /**
   * add to list of configsToIgnore if they are not meant to be validated.
   */
  @Before
  public void setUp() {
    configsToIgnore = new ArrayList<>();
  }

  @Test
  public void testConf() throws Exception {
    // get path to conf directory
    Path currentDir = Paths.get("").toAbsolutePath();
    Path confDir = currentDir.resolve("conf");
    assertNotNull(confDir);

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

        // check that all validations have no exceptions
        for (Map.Entry<String, List<Exception>> entry : exceptions.entrySet()) {
          assertEquals(0, entry.getValue().size());
        }
      }
    }
  }
}
