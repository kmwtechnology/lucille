package com.kmwllc.lucille.core;

import com.kmwllc.lucille.util.ThrowableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * A thread that runs a connector, publishing documents to a specified Publisher.
 *
 * <p>Any Throwable thrown by the connector, not only an Exception, is recorded as the thread's failure so that
 * {@link PublisherImpl#waitForCompletion} can fail the run instead of treating a thread that died as one that finished.
 * Another component can also {@link #abort} the thread, which records a failure and interrupts the connector.
 */
public class ConnectorThread extends Thread {

  private static final Logger log = LoggerFactory.getLogger(ConnectorThread.class);

  private final Connector connector;
  private final PublisherImpl publisher;
  private final String runId;

  private volatile Throwable failure;
  private volatile String failureMessage;

  public ConnectorThread(Connector connector, PublisherImpl publisher, String runId, String name) {
    this.setName(name);
    this.connector = connector;
    this.publisher = publisher;
    this.runId = runId;
  }

  @Override
  public void run() {
    MDC.put(Document.RUNID_FIELD, runId);
    try {
      connector.execute(publisher);
      publisher.flush();
    } catch (Throwable t) {
      markFailed("Connector exception.", t);
      if (t instanceof Error e && ThrowableUtils.isFatal(e)) {
        // the failure is recorded for the run; the JVM may be unusable, so do not swallow it
        throw e;
      }
    } finally {
      MDC.remove(Document.RUNID_FIELD);
    }
  }

  /**
   * Marks this connector thread as failed for the given reason and interrupts it, so that a run which can no longer
   * complete (for example because a worker died) ends promptly instead of waiting for the connector to finish.
   */
  public void abort(String message, Throwable cause) {
    markFailed(message, cause);
    interrupt();
  }

  private void markFailed(String message, Throwable cause) {
    // keep the first failure: a later one (such as the connector failing after being interrupted) is a consequence
    if (failure == null) {
      failure = cause;
      failureMessage = message;
    } else {
      log.debug("Ignoring subsequent connector failure", cause);
    }
  }

  /**
   * @return The Throwable that ended or aborted this thread, or null if it has not failed.
   */
  public Throwable getThrowable() {
    return failure;
  }

  /**
   * @return A short description of the failure, suitable for a run result, or null if the thread has not failed.
   */
  public String getFailureMessage() {
    return failureMessage;
  }

  /**
   * @return Whether this thread has failed or been aborted; see {@link #getThrowable()} for the cause.
   */
  public boolean isFailed() {
    return failure != null;
  }

}
