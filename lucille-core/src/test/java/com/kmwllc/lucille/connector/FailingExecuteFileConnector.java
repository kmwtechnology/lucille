package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.spec.Spec;
import com.typesafe.config.Config;

/**
 * Used to determine if the correct behavior happens when an Exception is thrown during execute().
 */
public class FailingExecuteFileConnector extends FileConnector {

  public static final Spec SPEC = FileConnector.SPEC;

  public FailingExecuteFileConnector(Config config) throws ConnectorException {
    super(config);
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    throw new ConnectorException("Expected");
  }
}
