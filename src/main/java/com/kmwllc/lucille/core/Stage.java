package com.kmwllc.lucille.core;

import java.util.List;

/**
 * An operation that can be performed on a Document.
 */
public abstract class Stage {

  public abstract List<Document> processDocument(Document doc) throws StageException;

}
