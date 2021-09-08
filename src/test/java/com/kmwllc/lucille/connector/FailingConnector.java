package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.Connector;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;

public class FailingConnector extends AbstractConnector {

  public FailingConnector(Config config) {
    super(config.hasPath("name") ? config.getString("name") : "FailingConnector",
      config.getString("pipeline"));
  }

  @Override
  public void start(Publisher publisher) throws ConnectorException {
    throw new ConnectorException("Expected.");
  }
}
