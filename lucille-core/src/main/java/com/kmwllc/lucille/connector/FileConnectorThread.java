package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import java.net.URI;
import java.util.concurrent.LinkedBlockingQueue;

public class FileConnectorThread extends Thread {

  private final FileConnector fileConnector;
  private final Publisher publisher;
  private ConnectorException caughtException;
  private final LinkedBlockingQueue<URI> queue;

  public FileConnectorThread(FileConnector fileConnector, Publisher publisher, LinkedBlockingQueue<URI> queue) {
    this.fileConnector = fileConnector;
    this.publisher = publisher;
    this.queue = queue;
  }

  @Override
  public void run() {
    URI resource;
    while ((resource = queue.poll()) != null) {
      try {
        fileConnector.traverseStoragePath(publisher, resource);
      } catch (ConnectorException e) {
        this.caughtException = e;
        return;
      }
    }
  }

  public ConnectorException getCaughtException() {
    return caughtException;
  }
}
