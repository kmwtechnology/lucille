package com.kmwllc.lucille.core;

/**
 * Represents the outcome of the Publisher's "wait for completion" process. Contains a status as
 * well as a message to be included in a Run summary.
 */
public class PublisherResult {

  private final boolean status;
  private final String message;

  public PublisherResult(boolean status, String message) {
    this.status = status;
    this.message = message;
  }

  public String getMessage() {
    return message;
  }

  public boolean getStatus() {
    return status;
  }
}
