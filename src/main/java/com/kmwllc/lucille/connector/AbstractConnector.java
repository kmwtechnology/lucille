package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.Connector;
import com.typesafe.config.Config;

public abstract class AbstractConnector implements Connector {

  private String name;
  private String pipelineName;

  public AbstractConnector(Config config) {
    this.name = config.getString("name");
    this.pipelineName = config.getString("pipeline");
  }

  public String getName() {
    return name;
  }

  public String getPipelineName() {
    return pipelineName;
  }

}
