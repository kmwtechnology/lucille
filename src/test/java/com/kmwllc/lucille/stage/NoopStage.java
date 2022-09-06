package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import java.util.List;

public class NoopStage extends AbstractStage {

  public NoopStage(Config config) {
    super(config);
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    return null;
  }
}
