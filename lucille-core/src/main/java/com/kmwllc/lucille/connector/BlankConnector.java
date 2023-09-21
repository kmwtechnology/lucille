package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connector implementation that produces blank documents given amount to produce
 */
public class BlankConnector extends AbstractConnector {

  private static final Logger log = LoggerFactory.getLogger(BlankConnector.class);
  private final int docNum;
  private final int docStartNum;

  public BlankConnector(Config config) {
    super(config);
    this.docNum = config.getInt("docNum");
    this.docStartNum = config.hasPath("docStartNum") ? config.getInt("docStartNum") : 0;
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    for (int i = 0; i < docNum; i++) {
      Document doc = Document.create(createDocId(Integer.toString(i + docStartNum)));
      try {
        publisher.publish(doc);
      } catch (Exception e) {
        throw new ConnectorException("Error creating or publishing document", e);
      }
    }
  }
}
