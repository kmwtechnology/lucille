package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.Connector;
import com.typesafe.config.Config;

public abstract class AbstractConnector implements Connector {

  private String name;
  private String pipelineName;
  private String docIdPrefix;

  public AbstractConnector(Config config) {
    this.name = config.getString("name");
    this.pipelineName = config.getString("pipeline");
    this.docIdPrefix = config.hasPath("docIdPrefix") ? config.getString("docIdPrefix") : "";
  }

  public String getName() {
    return name;
  }

  public String getPipelineName() {
    return pipelineName;
  }

  public String getDocIdPrefix() {
    return docIdPrefix;
  }

  /**
   * Creates an extended doc ID by adding a prefix (and possibly in the future, a suffix) to the
   * given id.
   */
  public String createDocId(String id) {
    return docIdPrefix + id;
  }
}
