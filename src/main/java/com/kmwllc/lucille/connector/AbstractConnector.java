package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.Connector;

public abstract class AbstractConnector implements Connector {

  private String name;
  private String pipelineName;

  public AbstractConnector(String name, String pipelineName) {
    this.name = name;
    this.pipelineName = pipelineName;
  }

  public String getName() {
    return name;
  }

  public String getPipelineName() {
    return pipelineName;
  }

}
