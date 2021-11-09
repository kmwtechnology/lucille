package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import java.util.List;

public class ErrorStage extends Stage {

  public ErrorStage(Config config) {
    super(config);
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {

    // bypass throwing an exception if the document has shouldFail==false
    if (doc.has("shouldFail") && "false".equals(doc.getString("shouldFail"))) {
      return null;
    }

    throw new StageException("Expected");
  }
}
