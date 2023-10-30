package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;

/**
 * Used to determine if the correct behavior happens when an Exception is thrown during execute().
 */
public class FailingExecuteCSVConnector extends CSVConnector {

  public FailingExecuteCSVConnector(Config config) {
    super(config);
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    throw new ConnectorException("Expected");
  }
}
