package com.kmwllc.lucille.opensearchIngest;

import com.kmwllc.lucille.core.Runner;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.util.List;
import java.util.Map;
import static org.junit.Assert.assertEquals;

import java.util.Objects;
import org.junit.Test;

public class OpensearchIngestExampleTest {
    @Test
    public void testConf() throws Exception {
      // obtain the conf directory in current working directory
      String relativeDirPath = "conf";
      File dir = new File(relativeDirPath).getAbsoluteFile();

      // check if the directory exists and is indeed a directory
      if (!dir.exists() || !dir.isDirectory()) {
        throw new RuntimeException("Directory " + relativeDirPath + " does not exist or is not a directory.");
      }

      // list all conf files in the directory
      File[] confFiles = dir.listFiles((d, name) -> name.endsWith(".conf"));

      // loop through each conf file
      for (File configFile : Objects.requireNonNull(confFiles)) {
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
