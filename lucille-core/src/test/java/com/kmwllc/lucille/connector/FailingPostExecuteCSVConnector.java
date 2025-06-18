package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.spec.Spec;
import com.typesafe.config.Config;

/**
 * Used to determine if the correct behavior happens when an Exception is thrown during postExecute().
 */
public class FailingPostExecuteCSVConnector extends CSVConnector {

  public static final Spec SPEC = Spec.connector().reqStr("path");

  public FailingPostExecuteCSVConnector(Config config) {
    super(config);
  }

  @Override
  public void postExecute(String runId) throws ConnectorException {
    throw new ConnectorException("Expected");
  }
}
