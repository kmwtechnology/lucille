package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;

public class NoOpConnector extends AbstractConnector {

  // static variable for use in testing; allows us to look at the publisher
  // supplied to this connector without having a reference to the connector instance
  private static Publisher suppliedPublisher = null;

  public static final Spec SPEC = SpecBuilder.connector().build();

  public NoOpConnector(Config config) {
    super(config);
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    this.suppliedPublisher = publisher;
  }

  public static Publisher getSuppliedPublisher() {
    return suppliedPublisher;
  }

  public static void reset() {
    suppliedPublisher = null;
  }
}
