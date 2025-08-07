package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import java.util.Iterator;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.typesafe.config.Config;

/**
 * Computes size of given byte array field and puts that size in a field on the Document.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>source (String) : The field containing the byte array.</li>
 *   <li>destination (String) : The name of the field which will be added to store the size. Will overwrite value if field already exists.</li>
 * </ul>
 */
public class ComputeFieldSize extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .requiredString("source", "dest").build();

  private final String source;
  private final String destination;

  public ComputeFieldSize(Config config) {
    super(config);

    this.source = config.getString("source");
    this.destination = config.getString("dest");
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

    byte[] bytes = doc.getBytes(source);
    if (bytes == null) {
      throw new StageException(String.format("Field: {} is not a byte array", source));
    }

    doc.update(destination, UpdateMode.OVERWRITE, bytes.length);
    return null;
  }
}
