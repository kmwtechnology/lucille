package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;

public class FailingConnector extends AbstractConnector {

  public static final Spec SPEC = SpecBuilder.connector().build();

  public FailingConnector(Config config) {
    super(config);
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    throw new ConnectorException("Expected.");
  }
}
