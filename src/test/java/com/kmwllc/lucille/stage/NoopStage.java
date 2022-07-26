package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import java.util.List;

public class NoopStage extends Stage {

  public NoopStage(Config config) {
    super(config);
  }

  @Override
  public List<JsonDocument> processDocument(JsonDocument doc) throws StageException {
    return null;
  }
}
