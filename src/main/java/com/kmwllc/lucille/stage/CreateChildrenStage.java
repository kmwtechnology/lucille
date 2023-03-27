package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import java.util.ArrayList;
import java.util.Iterator;

public class CreateChildrenStage extends Stage {

  private final int numChildren;
  private final boolean dropParent;
  private final Integer failAfter;

  public CreateChildrenStage(Config config) {
    super(config, new StageSpec().withOptionalProperties("numChildren", "dropParent", "failAfter"));
    this.numChildren = config.hasPath("numChildren") ? config.getInt("numChildren") : 3;
    this.dropParent = config.hasPath("dropParent") ? config.getBoolean("dropParent") : false;
    this.failAfter = config.hasPath("failAfter") ? config.getInt("failAfter") : null;
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

        if ((failAfter!=null) && (count >= failAfter)) {
          throw new RuntimeException();
        }

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
