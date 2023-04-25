package com.kmwllc.lucille.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Objects;

/**
 * Represents something that happened relating to a particular document in the context of a particular "run."
 * For example, a document may have been created during pipeline execution, or a document may have been indexed
 * in a search engine.
 */
public class Event {

  public enum Type {CREATE, FINISH, FAIL, DROP}

  private Type type;
  private String documentId;
  private String message;
  private String runId;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public Event(String documentId, String runId, String message, Type type) {
    this.documentId = documentId;
    this.runId = runId;
    this.message = message;
    this.type = type;
  }

  public Event(ObjectNode node) throws Exception {
    this.documentId = node.get("documentId").asText();
    this.message = node.get("message").asText();
    this.runId = node.get("runId").asText();
    this.type = Type.valueOf(node.get("type").asText());
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

  public Type getType() { return type; }

  public boolean isCreate() {
    return Type.CREATE.equals(type);
  }

  public String toString() {
    return "{\"documentId\": \"" + documentId + "\", \"runId\":\"" + runId + "\", \"message\": \"" +
      message + "\", \"type\": \"" + type +"\"}";
  }

  public static Event fromJsonString(String json) throws Exception {
    return new Event((ObjectNode)MAPPER.readTree(json));
  }

  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }

    if (!(o instanceof Event)) {
      return false;
    }

    Event e = (Event) o;
    return Objects.equals(documentId, e.documentId) &&
      Objects.equals(runId, e.runId) &&
      Objects.equals(message, e.message) &&
      Objects.equals(type, e.type);
  }

  public int hashCode() {
    return Objects.hash(documentId, runId, message, type);
  }

}
