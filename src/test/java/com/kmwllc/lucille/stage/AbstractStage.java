package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Stage;
import com.typesafe.config.Config;

import java.util.List;

public abstract class AbstractStage extends Stage {

  public AbstractStage(Config config) {
    super(config);
  }

  @Override
  public List<String> getPropertyList() {
    return null;
  }
}

