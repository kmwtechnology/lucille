package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Produces a sequence of empty Documents with numeric IDs.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>numDocs (Long, Required) : Total number of Documents to create.</li>
 *   <li>startWith (Int, Optional) : First ID value to use. Defaults to 0.</li>
 * </ul>
 */
public class SequenceConnector extends AbstractConnector {

  public static final Spec SPEC = SpecBuilder.connector()
      .requiredNumber("numDocs")
      .optionalNumber("startWith").build();

  private static final Logger log = LoggerFactory.getLogger(SequenceConnector.class);
  private final long numDocs;
  private final int startWith;

  public SequenceConnector(Config config) {
    super(config);

    this.numDocs = config.getLong("numDocs");
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
