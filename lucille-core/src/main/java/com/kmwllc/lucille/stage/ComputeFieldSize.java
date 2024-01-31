package com.kmwllc.lucille.stage;

import java.util.Iterator;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import com.amazonaws.services.s3.model.SelectObjectContentEvent.ContinuationEvent;
import com.kmwllc.lucille.core.*;

/**
 * Computes size of given byte array field and adds a field with that size
 * <br>
 * Config Parameters -
 * <br>
 * <p>
 * <b>source</b> (String) : The field containing the byte array
 * </p>
 * <p>
 * <b>destination</b> (String) : The name of the field which will be added to store the size. Will overwrite value if field already
 * exists
 * </p>
 */
public class ComputeFieldSize extends Stage {

  private final String source;
  private final String destination;

  public ComputeFieldSize(Config config) {
    super(config, new StageSpec().withRequiredProperties("source", "destination"));

    this.source = config.getString("source");
    this.destination = config.getString("destination");
  }

  /**
   * {@inheritDoc}
   * Here it processes document by computing and adding the correct field. 
   * @throws StageException If {@link ComputeFieldSize#source} does not exist in document
   * @throws NullPointerException If {@link ComputeFieldSize#source} is not a byte array in document
   */
  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (!doc.has(source)) {
      return null;
    }

    try {
      doc.update(destination, UpdateMode.OVERWRITE, doc.getBytes(source).length);
    } catch (NullPointerException e) {
      throw new StageException(String.format("Field: {} is not a byte array", source));
    }
    return null;
  }
}
