package com.kmwllc.lucille.core;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents the outcome of an execution of a specific connector feeding to a specific pipeline.
 * Contains a status as well as a message to be included in a run summary.
 */
// only the explicitly annotated properties are serialized, so that the message is not duplicated in the API output
@JsonAutoDetect(getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE)
public class ConnectorResult {

  private final String name;
  // true indicates success, false indicates failure
  private final boolean status;

  private final long numFailed;
  private final long numSucceeded;
  private final long numDropped;

  private final String message;

  public ConnectorResult(Connector connector, PublisherImpl publisher,
      boolean status, String errorMsg) {
    this(connector, publisher, status, errorMsg, null);
  }

  public ConnectorResult(Connector connector, PublisherImpl publisher,
      boolean status, String errMsg, Double durationSecs) {
    this.name = connector.getName();
    this.status = status;
    this.message = formatMessage(connector, publisher, status, errMsg, durationSecs);
    if (publisher != null) {
      this.numFailed = publisher.numFailed();
      this.numSucceeded = publisher.numSucceeded();
      this.numDropped = publisher.numDropped();
    } else {
      this.numFailed = 0;
      this.numSucceeded = 0;
      this.numDropped = 0;
    }
  }

  @JsonProperty("name")
  public String getName() {
    return name;
  }

  @JsonProperty("status")
  public boolean getStatus() {
    return status;
  }

  @JsonProperty("numSucceeded")
  public long getNumSucceeded() {
    return numSucceeded;
  }

  @JsonProperty("numFailed")
  public long getNumFailed() {
    return numFailed;
  }

  @JsonProperty("numDropped")
  public long getNumDropped() {
    return numDropped;
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

  private static String formatMessage(Connector connector, PublisherImpl publisher,
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
