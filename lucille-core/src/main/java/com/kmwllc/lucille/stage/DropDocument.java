package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.spec.Spec;
import com.typesafe.config.Config;
import java.util.Iterator;

/**
 * A stage that drops all documents that pass through it. Intended to use with the conditional framework.
 */
public class DropDocument extends Stage {

  public static final Spec SPEC = Spec.stage();

  public DropDocument(Config config) {
    super(config);
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    doc.setDropped(true);
    return null;
  }
}
