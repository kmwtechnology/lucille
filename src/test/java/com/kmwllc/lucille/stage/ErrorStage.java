package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import java.util.List;

public class ErrorStage extends Stage {

  private boolean exceptionOnStart = false;

  public ErrorStage(Config config) {
    super(config);
    if (config.hasPath("exceptionOnStart")&&config.getBoolean("exceptionOnStart")) {
      this.exceptionOnStart = true;
    }
  }

  @Override
  public void start() throws StageException {
    if (exceptionOnStart) {
      throw new StageException("Expected exception on start (thrown because exceptionOnStart==true)");
    }
  }

  @Override
  public List<JsonDocument> processDocument(JsonDocument doc) throws StageException {

    // bypass throwing an exception if the document has shouldFail==false
    if (doc.has("shouldFail") && "false".equals(doc.getString("shouldFail"))) {
      return null;
    }

    throw new StageException("Expected");
  }
}
