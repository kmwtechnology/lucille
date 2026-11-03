package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test connector that counts how many instances were constructed and how many were closed, so that tests can check
 * that every connector built for validation is closed again.
 */
public class CloseCountingConnector extends AbstractConnector {

  public static final Spec SPEC = SpecBuilder.connector().build();

  public static final AtomicInteger constructed = new AtomicInteger();
  public static final AtomicInteger closed = new AtomicInteger();

  public CloseCountingConnector(Config config) {
    super(config);
    constructed.incrementAndGet();
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
  }

  @Override
  public void close() throws ConnectorException {
    closed.incrementAndGet();
  }

  public static void reset() {
    constructed.set(0);
    closed.set(0);
  }
}
