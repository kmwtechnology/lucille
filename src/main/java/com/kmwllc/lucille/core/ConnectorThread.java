package com.kmwllc.lucille.core;

public class ConnectorThread extends Thread {

  private final Connector connector;
  private final Publisher publisher;

  private volatile Exception exception;

  public ConnectorThread(Connector connector, Publisher publisher) {
    this.connector = connector;
    this.publisher = publisher;
  }

  @Override
  public void run() {
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
