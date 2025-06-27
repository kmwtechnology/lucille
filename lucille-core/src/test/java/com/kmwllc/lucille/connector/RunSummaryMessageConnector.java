package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;

/**
 * Connector that sets a custom message to be included in the run summary.
 */
public class RunSummaryMessageConnector extends AbstractConnector {

  public static final String MESSAGE = "RunSummaryMessage123";

  public static final Spec SPEC = Spec.connector();

  public RunSummaryMessageConnector(Config config) {
    super(config);
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
  }

  @Override
  public void postExecute(String runId) throws ConnectorException {
    setMessage(MESSAGE);
  }
}
