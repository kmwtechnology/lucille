package com.kmwllc.lucille.core;

/**
 * Represents the outcome of an execution of a specific connector feeding to a specific pipeline.
 * Contains a status as well as a message to be included in a run summary.
 */
public class ConnectorResult {

  // true indicates success, false indicates failure
  private final boolean status;

  private final long numFailed;
  private final long numSucceeded;

  private String message;

  /**
   * Creates a ConnectorResult for the given connector / publisher, using the given status (representing whether it was
   * successful) and the given error Message.
   * @param connector The associated connector you want to create a result for.
   * @param publisher The associated publisher you want to create a result for.
   * @param status Whether the connector was successful.
   * @param errorMsg An error message associated with the connector / publisher's run.
   */
  public ConnectorResult(Connector connector, Publisher publisher,
      boolean status, String errorMsg) {
    this(connector, publisher, status, errorMsg, null);
  }

  /**
   * Creates a ConnectorResult for the given connector / publisher, using the given status (representing whether it was
   * successful) and the given error Message.
   * @param connector The associated connector you want to create a result for.
   * @param publisher The associated publisher you want to create a result for.
   * @param status Whether the connector was successful.
   * @param errMsg An error message associated with the connector / publisher's run.
   * @param durationSecs How long the connector took.
   */
  public ConnectorResult(Connector connector, Publisher publisher,
      boolean status, String errMsg, Double durationSecs) {
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

  /**
   * Get the status of this ConnectorResult.
   * @return the status of this ConnectorResult.
   */
  public boolean getStatus() {
    return status;
  }

  /**
   * Get whether this ConnectorResult had docs that failed.
   * @return whether this ConnectorResult had docs that failed.
   */
  public boolean hasFailingDocs() {
    return numFailed > 0;
  }

  /**
   * Get whether this ConnectorResult had docs.
   * @return whether this ConnectorResult had docs.
   */
  public boolean hasDocs() {
    return numFailed + numSucceeded > 0;
  }

  public String toString() {
    return message;
  }

  private static String formatMessage(Connector connector, Publisher publisher,
      boolean status, String errMsg, Double durationSecs) {
    String msg = connector.getName() + ": ";
    msg += status ? "complete. " : "ERROR. ";
    if (status) {
      if (publisher == null) {
        msg += "No pipeline configured.";
      } else {
        msg += publisher.numSucceeded() + " docs succeeded. " + publisher.numFailed() + " docs " +
            (publisher.numFailed() > 0 ? "FAILED." : "failed. " + publisher.numDropped() + " docs dropped.");
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
