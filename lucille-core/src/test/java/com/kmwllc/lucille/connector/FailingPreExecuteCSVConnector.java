package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.spec.Spec;
import com.typesafe.config.Config;

/**
 * Used to determine if the correct behavior happens when an Exception is thrown in preExecute().
 */
public class FailingPreExecuteCSVConnector extends CSVConnector {

  public static final Spec SPEC = Spec.connector().withRequiredProperties("path");

  public FailingPreExecuteCSVConnector(Config config) {
    super(config);
  }

  @Override
  public void preExecute(String runId) throws ConnectorException {
    throw new ConnectorException("Expected");
  }
}
