package com.kmwllc.lucille.testall;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import com.kmwllc.lucille.core.Runner;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;

public class TestAllExamples {

  List<String> configsToIgnore;

  /**
   * add to list of configsToIgnore if they are not meant to be validated. Note that if your configuration file uses any classes
   * from lucille plugins, add its dependency to this pom.xml
   */
  @Before
  public void setUp() {
    configsToIgnore = List.of(
      "csv-connector.conf", "simple-pipeline.conf", "json-connector.conf"
    );
  }

  @Test
  public void testConf() throws Exception {
    // get the current working directory
    Path currentDir = Paths.get("").toAbsolutePath();

    // get the parent directory
    Path parentDir = currentDir.getParent();
    assertNotNull(parentDir);

    try (Stream<Path> dirStream = Files.list(parentDir)) {
      List<Path> dirs = dirStream.filter(Files::isDirectory).toList();
      assertFalse(dirs.isEmpty());

      for (Path dir : dirs) {

        // get all conf files in examples directory
        if (!dir.getFileName().toString().equals("lucille-test-all-examples")) {
          Path confDir = dir.resolve("conf");
          if (Files.exists(confDir) && Files.isDirectory(confDir)) {
            try (Stream<Path> confStream = Files.list(confDir)) {
              List<Path> confPaths = confStream.filter(p -> p.toString().endsWith(".conf")).toList();
              assertFalse(confPaths.isEmpty());

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
      }
    }
  }
}
