package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConnectorException;
import com.typesafe.config.Config;

import java.time.Instant;

/**
 * This Class is only for test purposes! Used to determine if post completion actions occur how/when they are expected.
 */
public class PostCompletionCSVConnector extends CSVConnector {

  // Create static fields on the class that we can access from within our tests.
  private static boolean completionActionsOccurred = false;
  private static Instant postCompletionInstant;

  public PostCompletionCSVConnector(Config config) {
    super(config);
  }

  // This method should only be called after the pipeline has completed processing all of its documents.
  @Override
  public void postExecute(String runId) throws ConnectorException {
    completionActionsOccurred = true;
    postCompletionInstant = Instant.now();
  }

  // Getters for our static variables. These will help us determine if the postCompletionActions were executed as
  // expected, when expected
  public static boolean didPostCompletionActionsOccur() {
    return completionActionsOccurred;
  }
  public static Instant getPostCompletionInstant() {return postCompletionInstant;}
}
