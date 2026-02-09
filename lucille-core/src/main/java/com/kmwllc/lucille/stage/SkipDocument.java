package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
import java.util.Iterator;


/**
 * A stage that sets .skipped to true on the documents that pass through it. Intended to be used with the conditional framework
 * and the delete by query functionality.
 * By default, this stage moves to skip 100% of documents to the end of the pipeline and then send the document to the indexer.
 */
public class SkipDocument extends Stage {

  public static final Spec SPEC = SpecBuilder.stage().build();

  public SkipDocument(Config config) {
    super(config);
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    doc.setSkipped(true);
    return null;
  }
}
