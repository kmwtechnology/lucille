package com.kmwllc.lucille.util;

import com.kmwllc.lucille.connector.FileConnector;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import java.net.URI;

public class FileConnectorThread extends Thread {

  private final URI resource;
  private final FileConnector fileConnector;
  private final Publisher publisher;
  private ConnectorException caughtException;

  public FileConnectorThread(URI resource, FileConnector fileConnector, Publisher publisher) {
    this.resource = resource;
    this.fileConnector = fileConnector;
    this.publisher = publisher;
  }

  @Override
  public void run() {
    try {
      fileConnector.traverseStoragePath(publisher, resource);
    } catch (ConnectorException e) {
      this.caughtException = e;
    }
  }

  public ConnectorException getCaughtException() {
    return caughtException;
  }
}
