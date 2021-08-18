package com.kmwllc.lucille.callback;

import java.util.List;

/**
 * Connects to a data source, reads data, and generates Documents to be submitted for processing.
 */
public interface Connector {

  /**
   * An implementation of this method should perform any data-source specific logic for connecting,
   * acquiring data, and generating documents from it. All generated documents should be published
   * via the supplied Publisher
   *
   * @param publisher provides a publish() method accepting a document to be published
   */
  public void connect(Publisher publisher);

}
