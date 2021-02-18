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

  public abstract List<Document> processDocument(Document doc) throws StageException;

}
