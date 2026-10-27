package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;

/**
 * Test connector whose execute() throws an Error rather than an Exception, to check that a run notices.
 */
public class ErrorConnector extends AbstractConnector {

  public static final Spec SPEC = SpecBuilder.connector().build();

  public ErrorConnector(Config config) {
    super(config);
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    throw new StackOverflowError("Expected");
  }
}
