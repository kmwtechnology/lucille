package com.kmwllc.lucille.core;

import java.util.List;

/**
 * Represents the outcome of a Run. Contains a status as well as a message summarizing the Run.
 */
public class RunResult {

  // true indicates success, false indicates failure
  private final boolean status;
  private final String message;

  public RunResult(boolean status, List<Connector> connectors, List<ConnectorResult> connectorResults) {
    this.status = status;
    this.message = formatMessage(status, connectors, connectorResults);
  }

  public boolean getStatus() {
    return status;
  }

  public String toString() {
    return message;
  }

  private static String formatMessage(boolean status, List<Connector> connectors,
                                      List<ConnectorResult> connectorResults) {
    boolean failingDocs = connectorResults.stream().anyMatch(cr -> cr.hasFailingDocs());
    boolean anyDocs = connectorResults.stream().anyMatch(cr -> cr.hasDocs());
    StringBuffer sb = new StringBuffer();
    sb.append("\n\nRUN SUMMARY: ");
    if (status) {
      if (failingDocs) {
        sb.append("Partial success.");
      } else {
        sb.append("Success.");
      }
    } else {
      sb.append("Failure.");
    }
    long succesfulConnectors = connectorResults.stream().filter(cr -> cr.getStatus()).count();
    sb.append(" " + succesfulConnectors + "/" +  connectors.size() + " connectors complete. ");
    if (anyDocs) {
      if (failingDocs) {
        sb.append("Some docs failed.");
      } else {
        sb.append("All published docs succeeded.");
      }
    } else {
      sb.append("No docs published.");
    }
    sb.append("\n\n");
    for (ConnectorResult result : connectorResults) {
      sb.append(result.toString());
      sb.append("\n");
    }
    if (connectors.size()>0 && connectors.size()>connectorResults.size()) {
      for (int i=connectorResults.size(); i<connectors.size(); i++) {
        sb.append(connectors.get(i).getName() + ": skipped.\n");
      }
    }
    return sb.toString();
  }

}
