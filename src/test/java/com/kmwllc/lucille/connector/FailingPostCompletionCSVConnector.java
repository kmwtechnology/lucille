package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.connector.CSVConnector;
import com.kmwllc.lucille.core.ConnectorException;
import com.typesafe.config.Config;

/**
 * This Class is only for test purposes! Used to determine if the correct behavior happens when an Exception is thrown
 * during the execution of the post completion actions.
 */
public class FailingPostCompletionCSVConnector extends CSVConnector {


  public FailingPostCompletionCSVConnector(Config config) {
    super(config);
  }

  // This method should only be called after all of the documents are fully processed by the pipeline. This
  // implementation simulates an Exception being thrown during the execution of the post completion actions.
  @Override
  public void performPostCompletionActions() throws ConnectorException {
    throw new ConnectorException("Expected");
  }
}
