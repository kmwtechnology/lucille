package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConnectorException;
import com.typesafe.config.Config;

import java.time.Instant;

// TODO : Comment this to explain it is only for test purposes
public class PostCompletionCSVConnector extends CSVConnector {

  private static boolean completionActionsOccurred = false;
  private static Instant postCompletionInstant;

  public PostCompletionCSVConnector(Config config) {
    super(config);
  }

  @Override
  public void performPostCompletionActions() throws ConnectorException {
    completionActionsOccurred = true;
    postCompletionInstant = Instant.now();
  }

  public static boolean didPostCompletionActionsOccur() {
    return completionActionsOccurred;
  }

  public static Instant getPostCompletionInstant() {return postCompletionInstant;}
}
