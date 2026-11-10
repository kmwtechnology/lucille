package com.kmwllc.lucille.message;

/**
 * Thrown by a messenger when an operation on the messaging infrastructure fails: a document or event could not be
 * sent, a poll or commit failed, or the thread was interrupted while waiting (in which case the messenger restores its
 * interrupt flag before throwing).
 *
 * <p>For a send, a messenger marks the exception as specific to the document when the transport rejected this particular message
 * but is otherwise healthy, such as a Kafka record that is larger than the topic allows. The caller may then fail the
 * document and carry on with the next one. Any other send failure means the messenger can no longer be relied on, so
 * the caller should stop rather than continue sending through it.
 */
public class MessengerException extends Exception {

  private final boolean documentSpecific;

  public MessengerException(String message, Throwable cause) {
    this(message, cause, false);
  }

  public MessengerException(String message, Throwable cause, boolean documentSpecific) {
    super(message, cause);
    this.documentSpecific = documentSpecific;
  }

  /**
   * @return Whether the failure is specific to the document being sent, leaving the messenger usable for others.
   */
  public boolean isDocumentSpecific() {
    return documentSpecific;
  }
}
