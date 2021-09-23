package com.kmwllc.lucille.core;

public class ConnectorThread extends Thread {

  private final Connector connector;
  private final Publisher publisher;

  private volatile ConnectorException exception;

  public ConnectorThread(Connector connector, Publisher publisher) {
    this.connector = connector;
    this.publisher = publisher;
  }

  @Override
  public void run() {
    try {
      connector.execute(publisher);
    } catch (ConnectorException e) {
      exception = e;
    }
  }

  public ConnectorException getException() {
    return exception;
  }

  public boolean hasException() {
    return exception != null;
  }

}
