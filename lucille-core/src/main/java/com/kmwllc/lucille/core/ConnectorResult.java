package com.kmwllc.lucille.core;

/**
 * Represents the outcome of an execution of a specific connector feeding to a specific pipeline. Contains a status as well as a
 * message to be included in a run summary.
 */
public class ConnectorResult {

  // true indicates success, false indicates failure
  private final boolean status;

  private final int numFailed;
  private final int numSucceeded;

  private String message;

  public ConnectorResult(
      Connector connector, Publisher publisher, boolean status, String errorMsg) {
    this(connector, publisher, status, errorMsg, null);
  }

  public ConnectorResult(
      Connector connector,
      Publisher publisher,
      boolean status,
      String errMsg,
      Double durationSecs) {
    this.status = status;
    this.message = formatMessage(connector, publisher, status, errMsg, durationSecs);
    if (publisher != null) {
      this.numFailed = publisher.numFailed();
      this.numSucceeded = publisher.numSucceeded();
    } else {
      this.numFailed = 0;
      this.numSucceeded = 0;
    }
  }

  public boolean getStatus() {
    return status;
  }

  public boolean hasFailingDocs() {
    return numFailed > 0;
  }

  public boolean hasDocs() {
    return numFailed + numSucceeded > 0;
  }

  public String toString() {
    return message;
  }

  private static String formatMessage(
      Connector connector,
      Publisher publisher,
      boolean status,
      String errMsg,
      Double durationSecs) {
    String msg = connector.getName() + ": ";
    msg += status ? "complete. " : "ERROR. ";
    if (status) {
      if (publisher == null) {
        msg += "No pipeline configured.";
      } else {
        msg +=
            publisher.numSucceeded()
                + " docs succeeded. "
                + publisher.numFailed()
                + " docs "
                + (publisher.numFailed() > 0
                ? "FAILED."
                : "failed. " + publisher.numDropped() + " docs dropped.");
      }
    } else {
      msg += errMsg;
    }
    if (durationSecs != null) {
      msg = String.format("%s Time: %.2f secs.", msg, durationSecs);
    }

    if (connector.getMessage() != null) {
      msg += "\nMessage from " + connector.getName() + ": " + connector.getMessage();
    }

    return msg;
  }
}
