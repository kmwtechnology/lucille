package com.kmwllc.lucille.core;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.time.Instant;
import java.util.Objects;

/**
 * Represents something that happened relating to a particular document in the context of a particular "run."
 * For example, a document may have been created during pipeline execution, or a document may have been indexed
 * in a search engine.
 *
 * <p> The {@link #getType() type} says what happened. On a FAIL event the {@link #getMessage() message} is a
 * human-readable reason for the failure; its text is for people and may change between versions, so consumers should
 * act on the type rather than parse the message. The one exception is a document delivered to a Worker more times
 * than {@code worker.maxRetries} allows: its FAIL event always carries the message
 * {@value #WORKER_MAX_RETRIES_EXCEEDED}.
 *
 * <p> Two events are equal only if all of their fields are equal, including the instant at which each was created,
 * so events that record the same thing at different moments are not equal.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Event {

  public enum Type {CREATE, FINISH, FAIL, DROP}

  /**
   * The message of the FAIL event for a document delivered to a Worker more times than {@code worker.maxRetries}
   * allows. The Worker sends such a document to the dead letter queue instead of processing it again.
   */
  public static final String WORKER_MAX_RETRIES_EXCEEDED = "WORKER_MAX_RETRIES_EXCEEDED";

  private Type type;
  private String documentId;
  private String message;
  private String runId;
  private Instant instant = Instant.now();

  private String topic;
  private Integer partition;
  private Long offset;
  private String key;

  // Optional, config-driven subset of the source document's fields. Null (and therefore omitted from the
  // serialized JSON) unless event field enrichment is enabled and at least one requested field is present.
  // Stored as an ObjectNode so the fields serialize with the exact JSON shape they have in the document itself.
  private ObjectNode documentFields;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    MAPPER.registerModule(new JavaTimeModule());
  }

  private Event() {
  }

  public Event(Document document, String message, Type type) {
    this(document, message, type, null);
  }

  /**
   * Creates an Event for the given document, optionally embedding a subset of the document's fields as
   * described by {@code fieldConfig}. When {@code fieldConfig} is null or disabled, this behaves identically
   * to {@link #Event(Document, String, Type)} and no {@code documentFields} are included.
   */
  public Event(Document document, String message, Type type, EventFieldConfig fieldConfig) {
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
    if (fieldConfig != null) {
      this.documentFields = fieldConfig.extract(document);
    }
  }

  public Event(String documentId, String runId, String message, Type type) {
    this.documentId = documentId;
    this.runId = runId;
    this.message = message;
    this.type = type;
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

  public ObjectNode getDocumentFields() {
    return documentFields;
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

  /**
   * Describes a failure for the message of a FAIL event: the throwable's message, or its class name when it has none.
   * A wrapper that adds no message of its own, such as {@code new RuntimeException(cause)}, is described by its cause.
   */
  static String failureMessage(Throwable t) {
    while (t.getCause() != null && t.getCause().toString().equals(t.getMessage())) {
      t = t.getCause();
    }
    return t.getMessage() != null ? t.getMessage() : t.getClass().getName();
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
        Objects.equals(key, e.key) &&
        Objects.equals(documentFields, e.documentFields);
  }

  public int hashCode() {
    return Objects.hash(documentId, runId, message, type, instant, topic, partition, offset, key, documentFields);
  }

}
