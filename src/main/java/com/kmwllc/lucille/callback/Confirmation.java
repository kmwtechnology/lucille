package com.kmwllc.lucille.callback;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Objects;

public class Confirmation {

  private String documentId;
  private String message;
  private String runId;


  // TODO: types of confirmations: COMPLETED; ERROR; ADDED
  // confirmations can be expected or actual
  private boolean expected;



  private static final ObjectMapper MAPPER = new ObjectMapper();

  public Confirmation(String documentId, String runId, String message, boolean expected) {
    this.documentId = documentId;
    this.runId = runId;
    this.message = message;
    this.expected = expected;
  }

  public Confirmation(ObjectNode node) throws Exception {
    this.documentId = node.get("documentId").asText();
    this.message = node.get("message").asText();
    this.runId = node.get("runId").asText();
    this.expected = node.get("isExpected").asBoolean();
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

  public boolean isExpected() {
    return expected;
  }

  /**
   * message and isExpected are not included in equality
   *
   */
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }

    if (!(o instanceof Confirmation)) {
      return false;
    }

    Confirmation r = (Confirmation) o;
    return r.getDocumentId().equals(getDocumentId()) && r.getRunId().equals(getRunId());
  }

  public int hashCode() {
    return Objects.hash(documentId, runId);
  }

  public String toString() {
    return "{\"documentId\": \"" + documentId + "\", \"runId\":\"" + runId + "\", \"message\": \"" + message + "\", \"isExpected\":" + expected +"}";
  }

  public static Confirmation fromJsonString(String json) throws Exception {
    return new Confirmation((ObjectNode)MAPPER.readTree(json));
  }

  public static void main(String[] args) throws Exception {
    Confirmation r = new Confirmation("docId1", "runId1", "message1", false);
    System.out.println(r);
    System.out.println(Confirmation.fromJsonString(r.toString()));
  }

}
