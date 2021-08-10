package com.kmwllc.lucille.callback;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Objects;

public class Receipt {

  private String documentId;
  private String message;
  private String runId;
  private boolean open;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public Receipt(String documentId, String runId, String message, boolean open) {
    this.documentId = documentId;
    this.runId = runId;
    this.message = message;
    this.open = open;
  }

  public Receipt(ObjectNode node) throws Exception {
    this.documentId = node.get("documentId").asText();
    this.message = node.get("message").asText();
    this.runId = node.get("runId").asText();
    this.open = node.get("isOpen").asBoolean();
  }
  public String getDocumentId() {
    return documentId;
  }

  public String getRunId() {
    return runId;
  }

  public String getMessage() {
    return message;
  }

  public boolean isOpen() {
    return open;
  }

  /**
   * Note: message and isOpen are not included in equality
   *
   */
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }

    if (!(o instanceof Receipt)) {
      return false;
    }

    Receipt r = (Receipt) o;
    return r.getDocumentId().equals(getDocumentId()) && r.getRunId().equals(getRunId());
  }

  public int hashCode() {
    return Objects.hash(documentId, runId);
  }

  public String toString() {
    return "{\"documentId\": \"" + documentId + "\", \"runId\":\"" + runId + "\", \"message\": \"" + message + "\", \"isOpen\":" + open +"}";
  }

  public static Receipt fromJsonString(String json) throws Exception {
    return new Receipt((ObjectNode)MAPPER.readTree(json));
  }

  public static void main(String[] args) throws Exception {
    Receipt r = new Receipt("docId1", "runId1", "message1", false);
    System.out.println(r);
    System.out.println(Receipt.fromJsonString(r.toString()));
  }

}
