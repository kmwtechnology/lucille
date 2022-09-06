package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CreateChildrenStage extends Stage {
  public CreateChildrenStage(Config config) {
    super(config);
    if (config.hasPath("numChildren")) {
      numChildren = config.getInt("numChildren");
    }
  }

  private int numChildren = 3;

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    ArrayList<Document> children = new ArrayList();
    for (int i=0; i<numChildren; i++) {
      Document child = new Document(doc.getId()+ "_child" +i);
      children.add(child);
    }

    return children;
  }

  @Override
  public Set<String> getPropertyList() {
    return null;
  }
}
