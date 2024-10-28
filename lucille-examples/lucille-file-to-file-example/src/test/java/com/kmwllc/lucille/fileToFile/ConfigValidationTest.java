package com.kmwllc.lucille.fileToFile;

import com.kmwllc.lucille.test.ConfigValidationUtils;
import java.util.List;
import org.junit.Test;

public class ConfigValidationTest {

  @Test
  public void testConf() throws Exception {
    List<String> configsToIgnore = List.of("csv-connector.conf", "json-connector.conf", "simple-pipeline.conf");
    ConfigValidationUtils.validateConfigs(configsToIgnore);
  }
}
