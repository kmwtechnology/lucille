package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

public class ExtractEntitiesFSTTest extends ExtractEntitiesTest {

  @Override
  protected Stage newStage(String hoconBody) {
    try {
      Config cfg = ConfigFactory.parseString(hoconBody).resolve()
          .withValue("class", ConfigValueFactory.fromAnyRef("com.kmwllc.lucille.stage.ExtractEntitiesFST"));
      Stage stage = new ExtractEntitiesFST(cfg);
      stage.start();

      return stage;
    } catch (Exception e) {
      throw new RuntimeException(new StageException("Failed to instantiate stage: ExtractEntitiesFST", e));
    }
  }
}





