package com.kmwllc.lucille.testall;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.kmwllc.lucille.core.Runner;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
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
    // Get the current working directory
    Path currentDir = Paths.get("").toAbsolutePath();

    // Get the parent directory
    Path parentDir = currentDir.getParent();
    assertNotNull(parentDir);

    File[] directories = parentDir.toFile().listFiles(File::isDirectory);
    assertNotNull(directories);

    for (File dir : directories) {
      if (!dir.getName().equals("lucille-test-all-examples")) {
        File confDir = new File(dir, "conf");
        if (confDir.exists() && confDir.isDirectory()) {
          // list all conf files in the directory
          File[] confFiles = confDir.listFiles((d, name) -> name.endsWith(".conf"));
          assertNotNull(confFiles);

          for (File configFile : confFiles) {
            if (configsToIgnore.contains(configFile.getName())) continue;
            // run the configFile and gather exceptions
            Config config = ConfigFactory.parseFile(configFile);
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
