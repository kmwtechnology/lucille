package com.kmwllc.lucille.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigResolveOptions;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

public class CamelCaseConfigConverterTest {
  @Test
  public void testConvert() {
    Path configPath = Paths.get("src/test/resources/CamelCaseConfigConverterTest/config.conf");
    CamelCaseConfigConverter.main(new String[]{configPath.toAbsolutePath().toString()});
    // check that the new file was created
    File camelCaseConfigFile = null;
    try {
      camelCaseConfigFile = new File("src/test/resources/CamelCaseConfigConverterTest/configCamelCase.conf");
      assertTrue(camelCaseConfigFile.exists());
      Config camelCaseConfig = ConfigFactory.parseFile(camelCaseConfigFile).resolve();

      // test only converts the first instance of stage property in a single line
      assertEquals("path/to/tika/config/with/tika_config_path/tika.xml",
          camelCaseConfig.getConfigList("pipelines").get(0).getConfigList("stages").get(0).getString("tikaConfigPath"));
      assertEquals("byte_array_field",
          camelCaseConfig.getConfigList("pipelines").get(0).getConfigList("stages").get(0).getString("byteArrayField"));

      // test that does not affect property values
      Map<String, Object> fieldMapping = camelCaseConfig.getConfigList("pipelines")
          .get(0).getConfigList("stages").get(1).getObject("fieldMapping").unwrapped();
      assertEquals(
          Map.of("dc_subject", "subject", "message_to", "to", "message_from", "from"), fieldMapping);

      assertEquals(
          List.of("file_content", "file_size_bytes", "file_creation_date"), camelCaseConfig.getConfigList("pipelines")
              .get(0).getConfigList("stages").get(2).getStringList("fields"));

      // test that convertor did not change other properties not in stages
      assertNotNull(camelCaseConfig.getConfigList("connectors").get(0).getString("vfs_path"));

      // test if works in second pipeline
      assertNotNull(camelCaseConfig.getConfigList("pipelines").get(1).getConfigList("stages").get(1).getObject("staticValues"));

    } finally {
      if (camelCaseConfigFile != null && camelCaseConfigFile.exists()) {
        camelCaseConfigFile.delete();
      }
    }

  }
}
