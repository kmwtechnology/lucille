package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import java.util.ArrayList;
import java.util.List;

public class CreateChildrenStage extends Stage {
  public CreateChildrenStage(Config config) {
    super(config);
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    ArrayList<Document> children = new ArrayList();
    for (int i=0; i<3; i++) {
      Document child = doc.clone();
      child.setField(Document.ID_FIELD, doc.getId()+ "_child" +i);
      children.add(child);
    }

    return children;
  }
}
