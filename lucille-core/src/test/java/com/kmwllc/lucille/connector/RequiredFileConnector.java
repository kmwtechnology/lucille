package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
import java.io.File;

/**
 * Test connector whose constructor throws if requiredFile doesn't exist. Deleting the file after a config is
 * validated makes the run fail before it starts, with an exception rather than a RunResult.
 */
public class RequiredFileConnector extends AbstractConnector {

  public static final Spec SPEC = SpecBuilder.connector().requiredString("requiredFile").build();

  public RequiredFileConnector(Config config) throws ConnectorException {
    super(config);
    String requiredFile = config.getString("requiredFile");
    if (!new File(requiredFile).exists()) {
      throw new ConnectorException("Required file not found: " + requiredFile);
    }
  }

  @Override
  public void execute(Publisher publisher) {
  }
}
