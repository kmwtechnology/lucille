package com.kmwllc.lucille.fileToFile;

import com.kmwllc.lucille.test.ConfigValidationUtils;
import java.util.List;
import org.junit.Test;

public class ConfigValidationTest {

  @Test
  public void testConf() throws Exception {
    List<String> configsToIgnore = List.of("connector-for-csv.conf", "connector-for-json.conf", "simple-pipeline.conf");
    ConfigValidationUtils.validateConfigs(configsToIgnore);
  }
}
