import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.PresetConfigHandler;
import com.typesafe.config.Config;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.junit.Test;

public class PresetConfigHandlerTest {

  Path presetsPath = Paths.get("src/test/resources/presets");
  Path config1Path = Paths.get("src/test/resources/presets/config1.conf");

  @Test
  public void testFetchConfigs() throws Exception {
    PresetConfigHandler handler = new PresetConfigHandler(presetsPath);
    Map<String, Config> presetMap = handler.fetchConfigs();

    assertEquals(4, presetMap.size());

    assertEquals(1, presetMap.get("config1.conf").getInt("id"));

    // Flexing identical filenames (but one conf, one json)
    assertEquals(2, presetMap.get("config2.conf").getInt("id"));
    assertEquals("conf", presetMap.get("config2.conf").getString("from"));

    assertEquals(2, presetMap.get("config2.json").getInt("id"));
    assertEquals("JSON", presetMap.get("config2.json").getString("from"));

    assertEquals(3, presetMap.get("config3.json").getInt("id"));

    assertFalse(presetMap.containsKey("hello.txt"));
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
