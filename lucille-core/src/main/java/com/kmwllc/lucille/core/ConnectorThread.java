package com.kmwllc.lucille.core;

import org.slf4j.MDC;

/**
 * A thread that runs a connector, publishing documents to a specified Publisher.
 */
public class ConnectorThread extends Thread {

  private final Connector connector;
  private final Publisher publisher;
  private final String runId;

  private volatile Exception exception;

  /**
   * Creates a ConnectorThread from the given arguments.
   *
   * @param connector The connector to execute.
   * @param publisher The publisher to publish documents to.
   * @param runId The runId associated with this thread.
   * @param name The name of the thread.
   */
  public ConnectorThread(Connector connector, Publisher publisher, String runId, String name) {
    this.setName(name);
    this.connector = connector;
    this.publisher = publisher;
    this.runId = runId;
  }

  @Override
  public void run() {
    MDC.put("run_id", runId);
    try {
      connector.execute(publisher);
      publisher.flush();
    } catch (Exception e) {
      exception = e;
    }
  }

  /**
   * Get the exception associated with this connector thread.
   * @return the exception associated with this connector thread.
   */
  public Exception getException() {
    return exception;
  }

  /**
   * Returns whether this thread has an exception.
   * @return whether this thread has an exception.
   */
  public boolean hasException() {
    return exception != null;
  }

}
