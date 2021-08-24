package com.kmwllc.lucille.core;

import com.typesafe.config.Config;

import java.util.List;

/**
 * An operation that can be performed on a Document.
 */
public abstract class Stage {

  protected Config config;

  public Stage(Config config) {
    this.config = config;
  }

  protected void start() throws StageException {
  }

  /**
   * Applies an operation to a Document in place and returns a list containing any child Documents generated
   * by the operation. If no child Documents are generated, the return value should be null.
   *
   * This interface assumes that the list of child Documents is large enough to hold in memory. To support
   * an unbounded number of child documents, this method would need to return an Iterator (or something similar)
   * instead of a List.
   */
  public abstract List<Document> processDocument(Document doc) throws StageException;

}
