package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import java.util.Iterator;

/**
 * Creates a designated number of children documents.
 * <br> This stage is intended for use in testing only.
 * <br> It is included in the main source tree so that it can be used in manual tests run against an artifact
 * that excludes the test tree.
 *
 */
public class CreateChildrenStage extends Stage {

  public static Spec SPEC = Spec.stage()
      .optNum("numChildren", "failAfter", "dropChild")
      .optBool("dropParent");

  private final int numChildren;
  private final boolean dropParent;
  private final Integer failAfter;
  private final Integer dropChild;

  public CreateChildrenStage(Config config) {
    super(config);
    this.numChildren = config.hasPath("numChildren") ? config.getInt("numChildren") : 3;
    this.dropParent = config.hasPath("dropParent") ? config.getBoolean("dropParent") : false;
    this.failAfter = config.hasPath("failAfter") ? config.getInt("failAfter") : null;
    this.dropChild = config.hasPath("dropChild") ? config.getInt("dropChild") : null;
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

        if ((failAfter != null) && (count >= failAfter)) {
          throw new RuntimeException();
        }

        if (count >= numChildren) {
          return null;
        }
        count = count + 1;
        Document child = Document.create(doc.getId() + "_child" + count);

        if ((dropChild != null) && (count == dropChild)) {
          child.setDropped(true);
        }

        return child;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }
}
