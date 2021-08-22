package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Pipeline;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

@RunWith(JUnit4.class)
public class AbstractStageTest {
  private Config config;
  
  public Pipeline loadConfig(String configPath) throws Exception {
    URI configUri = this.getClass().getClassLoader().getResource(configPath).toURI();
    String configStr = Files.readString(Paths.get(configUri), StandardCharsets.UTF_8);
    config = ConfigFactory.parseString(configStr);
    Pipeline pipeline = Pipeline.fromConfig(config);
    return pipeline;
  }
  
  protected Config getConfig() {
    return this.config;
  }
}
