package com.kmwllc.lucille.fileToFile;

import com.kmwllc.lucille.test.TestConfUtils;
import java.util.List;
import org.junit.Test;

public class TestConfigs {

  @Test
  public void testConf() throws Exception {
    List<String> configsToIgnore = List.of("csv-connector.conf", "json-connector.conf", "simple-pipeline.conf");
    TestConfUtils.validateConfigs(configsToIgnore);
  }
}
