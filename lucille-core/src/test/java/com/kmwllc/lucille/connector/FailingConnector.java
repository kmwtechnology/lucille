package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConfigSpec;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;

public class FailingConnector extends AbstractConnector {

  public FailingConnector(Config config) {
    super(config, ConfigSpec.forConnector());
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    throw new ConnectorException("Expected.");
  }
}
