package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Stage;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.lang.reflect.Constructor;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class BaseStageTest {
  private Config config;
  
  
  // Instantiate only the 1st stage from the config and return it.
  // Stage.start() is called before returning the stage.
  // TODO: Warn or Error if there's more than 1 stage?
  public Stage loadConfig(String configPath) throws Exception {
    URI configUri = this.getClass().getClassLoader().getResource(configPath).toURI();
    String configStr = Files.readString(Paths.get(configUri), StandardCharsets.UTF_8);
    config = ConfigFactory.parseString(configStr);
    List<? extends Config> stages = config.getConfigList("stages");
    if (stages.size() > 0) {
      Config stageConfig = stages.get(0);
      Class<?> clazz = Class.forName(stageConfig.getString("class"));
      Constructor<?> constructor = clazz.getConstructor(Config.class);
      Stage stage = (Stage) constructor.newInstance(stageConfig);
      stage.start();
      return stage;
    }
    return null;
  }
  
  protected Config getConfig() {
    return this.config;
  }
}
