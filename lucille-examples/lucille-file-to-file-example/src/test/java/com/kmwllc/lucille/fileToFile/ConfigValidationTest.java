package com.kmwllc.lucille.fileToFile;

import java.util.List;

import org.junit.Test;

import com.kmwllc.lucille.test.ConfigValidationUtils;

public class ConfigValidationTest {

  @Test
  public void testConf() throws Exception {
    List<String> configsToIgnore = List.of("csv-connector.conf", "json-connector.conf", "simple-pipeline.conf");
    ConfigValidationUtils.validateConfigs(configsToIgnore);
  }
}
