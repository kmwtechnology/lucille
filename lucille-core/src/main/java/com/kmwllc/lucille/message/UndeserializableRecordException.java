package com.kmwllc.lucille.message;

/**
 * Thrown by a messenger when a record it polled can't be deserialized into a Document, and it is not configured to skip
 * the record. The message gives the record's location. The record stays in place, so polling again fails the same way,
 * and a restarted consumer resumes at it.
 */
public class UndeserializableRecordException extends MessengerException {

  public UndeserializableRecordException(String message, Throwable cause) {
    super(message, cause);
  }
}
