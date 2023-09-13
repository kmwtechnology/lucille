package com.kmwllc.lucille.core;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.solr.common.annotation.JsonProperty;

import java.time.Instant;
import java.util.Objects;

/**
 * Represents something that happened relating to a particular document in the context of a particular "run."
 * For example, a document may have been created during pipeline execution, or a document may have been indexed
 * in a search engine.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Event {

  public enum Type {CREATE, FINISH, FAIL, DROP}

  private Type type;
  private String documentId;
  private String message;
  private String runId;
  private Instant instant = Instant.now();

  private String topic;
  private Integer partition;
  private Long offset;
  private String key;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    MAPPER.registerModule(new JavaTimeModule());
  }

  private Event() {
  }

  public Event(Document document, String message, Type type) {
    this.documentId = document.getId();
    this.runId = document.getRunId();
    this.message = message;
    this.type = type;
    if (document instanceof KafkaDocument) {
      KafkaDocument kafkaDocument = (KafkaDocument) document;
      this.topic = kafkaDocument.getTopic();
      this.partition = kafkaDocument.getPartition();
      this.offset = kafkaDocument.getOffset();
      this.key = kafkaDocument.getKey();
    }
  }

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

  public Type getType() {
    return type;
  }

  public Instant getInstant() {
    return instant;
  }

  public String getTopic() {
    return topic;
  }

  public Integer getPartition() {
    return partition;
  }

  public Long getOffset() {
    return offset;
  }

  public String getKey() {
    return key;
  }

  @JsonIgnore
  public boolean isCreate() {
    return Type.CREATE.equals(type);
  }

  public String toString() {
    try {
      return MAPPER.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Event could not be serialized.");
    }
  }

  public static Event fromJsonString(String json) throws Exception {
    return MAPPER.readValue(json, Event.class);
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
        Objects.equals(type, e.type) &&
        Objects.equals(instant, e.instant) &&
        Objects.equals(topic, e.topic) &&
        Objects.equals(partition, e.partition) &&
        Objects.equals(offset, e.offset) &&
        Objects.equals(key, e.key);
  }

  public int hashCode() {
    return Objects.hash(documentId, runId, message, type, instant, topic, partition, offset, key);
  }

}
