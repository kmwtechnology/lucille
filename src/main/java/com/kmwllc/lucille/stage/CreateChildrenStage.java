package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import java.util.ArrayList;
import java.util.Iterator;

public class CreateChildrenStage extends Stage {

  private int numChildren = 3;
  private boolean dropParent = false;

  public CreateChildrenStage(Config config) {
    super(config, new StageSpec().withOptionalProperties("numChildren", "dropParent"));
    if (config.hasPath("numChildren")) {
      numChildren = config.getInt("numChildren");
    }
    if (config.hasPath("dropParent")) {
      this.dropParent = config.getBoolean("dropParent");
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {

    if (dropParent) {
      doc.setDropped(true);
    }

    return new Iterator<>() {
      int count = 0;

      @Override
      public boolean hasNext() {
        return count < numChildren;
      }

      @Override
      public Document next() {
        if (count >= numChildren) {
          return null;
        }
        count = count + 1;
        return Document.create(doc.getId() + "_child" + count);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }
}
