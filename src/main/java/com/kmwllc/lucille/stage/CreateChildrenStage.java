package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CreateChildrenStage extends Stage {
  public CreateChildrenStage(Config config) {
    super(config);
    if (config.hasPath("numChildren")) {
      numChildren = config.getInt("numChildren");
    }
  }

  private int numChildren = 3;

  @Override
  public List<JsonDocument> processDocument(JsonDocument doc) throws StageException {
    ArrayList<JsonDocument> children = new ArrayList();
    for (int i=0; i<numChildren; i++) {
      JsonDocument child = new JsonDocument(doc.getId()+ "_child" +i);
      children.add(child);
    }

    return children;
  }
}
