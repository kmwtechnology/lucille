package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
import java.util.Iterator;


/**
 * Marks all documents as skipped. Intended to be used with Conditions to determine which documents will be affected.
 * By default, this stage moves to skip 100% of documents to the end of the pipeline and then sends the document to the indexer.
 *
 * Intended for a deletion use case where a Document representing a delete operation should reach the Indexer
 *  but should not be processed by downstream Stages in the pipeline.
 *
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
