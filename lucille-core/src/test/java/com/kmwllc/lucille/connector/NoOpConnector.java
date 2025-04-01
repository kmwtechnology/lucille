package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConfigSpec;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;

public class NoOpConnector extends AbstractConnector {

  // static variable for use in testing; allows us to look at the publisher
  // supplied to this connector without having a reference to the connector instance
  private static Publisher suppliedPublisher = null;

  public NoOpConnector(Config config) {
    super(config, ConfigSpec.forConnector());
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
