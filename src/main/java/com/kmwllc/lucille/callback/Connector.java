package com.kmwllc.lucille.callback;

import java.util.List;

/**
 * Connects to a data source, reads data, and generates Documents to be submitted for processing.
 */
public interface Connector {

  /**
   * An implementation of this method should perform any data-source specific logic for connecting,
   * acquiring data, and generating documents from it. This method must provide two guarantees:
   * 1) the IDs of all generated documents must be added to the documentIds list,
   * 2) all generated documents must be submitted for processing via the submitForProcessing() method
   * on the provided ConnectorDocumentManager
   *
   * @param documentIds a list into which the connector should add the IDs of all documents it generates
   * @param documentManager a ConnectorDocumentManager that provides a method for submitting generated documents
   */
  public void connect(List<String> documentIds, ConnectorDocumentManager documentManager);

}
