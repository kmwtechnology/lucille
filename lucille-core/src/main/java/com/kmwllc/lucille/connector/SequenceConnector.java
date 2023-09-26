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
public class SequenceConnector extends AbstractConnector {

  private static final Logger log = LoggerFactory.getLogger(SequenceConnector.class);
  private final int numDocs;
  private final int startWith;

  public SequenceConnector(Config config) {
    super(config);
    this.numDocs = config.getInt("numDocs");
    this.startWith = config.hasPath("startWith") ? config.getInt("startWith") : 0;
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    for (int i = 0; i < numDocs; i++) {
      Document doc = Document.create(createDocId(Integer.toString(i + startWith)));
      try {
        publisher.publish(doc);
      } catch (Exception e) {
        throw new ConnectorException("Error creating or publishing document", e);
      }
    }
  }
}
