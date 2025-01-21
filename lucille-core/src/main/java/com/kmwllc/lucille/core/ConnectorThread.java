package com.kmwllc.lucille.core;

import org.apache.logging.log4j.ThreadContext;

public class ConnectorThread extends Thread {

  private final Connector connector;
  private final Publisher publisher;
  private final String runId;

  private volatile Exception exception;

  public ConnectorThread(Connector connector, Publisher publisher, String runId, String name) {
    this.setName(name);
    this.connector = connector;
    this.publisher = publisher;
    this.runId = runId;
  }

  @Override
  public void run() {
    ThreadContext.put("run_id", runId);
    try {
      connector.execute(publisher);
      publisher.flush();
    } catch (Exception e) {
      exception = e;
    }
  }

  public Exception getException() {
    return exception;
  }

  public boolean hasException() {
    return exception != null;
  }

}
