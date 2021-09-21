package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.connector.CSVConnector;
import com.kmwllc.lucille.core.ConnectorException;
import com.typesafe.config.Config;

public class FailingPostCompletionCSVConnector extends CSVConnector {

  private static boolean completionActionsOccurred = false;

  public FailingPostCompletionCSVConnector(Config config) {
    super(config);
  }

  @Override
  public void performPostCompletionActions() throws ConnectorException {
    throw new ConnectorException("Expected");
  }

  public static boolean didPostCompletionActionsOccur() {
    return completionActionsOccurred;
  }
}
