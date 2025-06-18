package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.spec.Spec;
import com.typesafe.config.Config;

import java.util.Iterator;
import java.util.List;

public class ErrorStage extends Stage {

  public static Spec SPEC = Spec.stage().optBool("exceptionOnStart");

  private boolean exceptionOnStart = false;

  public ErrorStage(Config config) {
    super(config);
    if (config.hasPath("exceptionOnStart") && config.getBoolean("exceptionOnStart")) {
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
  public Iterator<Document> processDocument(Document doc) throws StageException {

    // bypass throwing an exception if the document has shouldFail==false
    if (doc.has("shouldFail") && "false".equals(doc.getString("shouldFail"))) {
      return null;
    }

    throw new StageException("Expected");
  }
}
