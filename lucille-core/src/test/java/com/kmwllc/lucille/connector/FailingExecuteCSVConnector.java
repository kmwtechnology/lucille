package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;

/**
 * Used to determine if the correct behavior happens when an Exception is thrown during execute().
 */
public class FailingExecuteCSVConnector extends CSVConnector {

  public static final Spec SPEC = SpecBuilder.connector().requiredString("path").build();

  public FailingExecuteCSVConnector(Config config) {
    super(config);
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    throw new ConnectorException("Expected");
  }
}
