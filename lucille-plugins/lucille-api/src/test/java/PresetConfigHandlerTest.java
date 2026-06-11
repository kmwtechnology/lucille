import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.PresetConfigHandler;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.junit.Test;

public class PresetConfigHandlerTest {

  Path presetsPath = Paths.get("src/test/resources/test_presets");
  Path config1Path = Paths.get("src/test/resources/test_presets/config1.conf");

  @Test
  public void testFetchConfigs() throws Exception {
    String prevDummy = System.getProperty("DUMMY_VALUE");

    try {
      System.setProperty("DUMMY_VALUE", "my_property");
      System.out.println(ConfigFactory.systemProperties().getString("DUMMY_VALUE"));
      PresetConfigHandler handler = new PresetConfigHandler(presetsPath);
      Map<String, Config> presetMap = handler.fetchConfigs();

      assertEquals(5, presetMap.size());

      assertEquals(1, presetMap.get("config1.conf").getInt("id"));
      // flexing that environment variables are resolved properly
      assertEquals("my_property", presetMap.get("config1.conf").getString("environmentValue"));
      // and that including other configs is as well
      assertTrue(presetMap.get("config1.conf").getObject("object").containsKey("key"));

      // Flexing identical filenames (but one conf, one json)
      assertEquals(2, presetMap.get("config2.conf").getInt("id"));
      assertEquals("conf", presetMap.get("config2.conf").getString("from"));

      assertEquals(2, presetMap.get("config2.json").getInt("id"));
      assertEquals("JSON", presetMap.get("config2.json").getString("from"));

      assertEquals(2, presetMap.get("config2.hocon").getInt("id"));
      assertEquals("hocon", presetMap.get("config2.hocon").getString("from"));

      assertEquals(3, presetMap.get("config3.json").getInt("id"));

      assertFalse(presetMap.containsKey("hello.txt"));
      // a malformed conf file in the directory. a warning is just logged,
      // this does not cause a crash & should not be added to the map
      assertFalse(presetMap.containsKey("bad.conf"));
    } finally {
      if (prevDummy != null) {
        System.setProperty("DUMMY_VALUE", prevDummy);
      } else {
        System.clearProperty("DUMMY_VALUE");
      }
    }
  }

  @Test
  public void testNonDirectoryPath() {
    PresetConfigHandler badHandler = new PresetConfigHandler(config1Path);
    assertThrows(IOException.class, () -> badHandler.fetchConfigs());
  }

  @Test
  public void testNullPath() throws Exception {
    PresetConfigHandler noPath = new PresetConfigHandler(null);
    Map<String, Config> fetched = noPath.fetchConfigs();
    assertTrue(fetched.isEmpty());
  }
}
