package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.spec.Spec;
import com.typesafe.config.Config;

/**
 * Used to determine if the correct behavior happens when an Exception is thrown during postExecute().
 */
public class FailingPostExecuteFileConnector extends FileConnector {

  public static final Spec SPEC = FileConnector.SPEC;

  public FailingPostExecuteFileConnector(Config config) throws ConnectorException {
    super(config);
  }

  @Override
  public void postExecute(String runId) throws ConnectorException {
    throw new ConnectorException("Expected");
  }
}
