package com.kmwllc.lucille.connector;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.spec.Spec;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Connector implementation that produces a certain number of empty Documents. Each Document will have a number for its ID
 * (generated in order).
 *
 * <p> Config Parameters:
 * <ul>
 *   <li>numDocs (Long): The number of Documents you want to create.</li>
 *   <li>startWith (Int, Optional): The ID you want the first Document to have. Defaults to zero.</li>
 * </ul>
 */
public class SequenceConnector extends AbstractConnector {

  public static final Spec SPEC = Spec.connector()
      .requiredNumber("numDocs")
      .optionalNumber("startWith");

  private static final Logger log = LoggerFactory.getLogger(SequenceConnector.class);

  private long numDocs;
  private int startWith;

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
