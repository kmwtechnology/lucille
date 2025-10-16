package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import java.lang.reflect.Constructor;

public class ExtractEntitiesAhoTest extends ExtractEntitiesTest {

  @Override
  protected Stage newStage(String hoconBody) {
    try {
      Config cfg = ConfigFactory.parseString(hoconBody).resolve()
          .withValue("class", ConfigValueFactory.fromAnyRef("com.kmwllc.lucille.stage.ExtractEntities"));
      Class<? extends Stage> cls = com.kmwllc.lucille.stage.ExtractEntities.class;
      Constructor<? extends Stage> ctor = cls.getConstructor(com.typesafe.config.Config.class);
      Stage stage = ctor.newInstance(cfg);
      stage.start();

      return stage;
    } catch (Exception e) {
      throw new RuntimeException(new StageException("Failed to instantiate stage: ExtractEntities", e));
    }
  }
}