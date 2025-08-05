package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;

public class SleepConnector extends AbstractConnector {

  private final int duration;

  public static final Spec SPEC = Spec.connector().requiredNumber("duration");

  public SleepConnector(Config config) {
    super(config);
    this.duration = config.getInt("duration");
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    try {
      Thread.sleep(duration);
    } catch (InterruptedException e) {
      throw new ConnectorException("Sleep was interrupted", e);
    }
  }
}
