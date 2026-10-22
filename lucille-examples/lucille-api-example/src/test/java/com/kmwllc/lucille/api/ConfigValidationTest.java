package com.kmwllc.lucille.api;

import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Runner;
import com.kmwllc.lucille.test.ConfigValidationUtils;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class ConfigValidationTest {

  @Test
  public void testConf() throws Exception {
    ConfigValidationUtils.validateConfigs();
  }

  // ConfigValidationUtils only picks up .conf files
  @Test
  public void testJsonConf() throws Exception {
    Map<String, List<Exception>> exceptions = Runner.runInValidationMode(ConfigFactory.parseFile(new File("conf/run_b.json")));
    assertTrue("configuration validation failed for run_b.json: " + exceptions,
        exceptions.values().stream().allMatch(List::isEmpty));
  }
}
