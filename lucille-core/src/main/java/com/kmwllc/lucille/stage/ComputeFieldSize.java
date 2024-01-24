package com.kmwllc.lucille.stage;

import java.util.Iterator;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import com.kmwllc.lucille.core.*;

/**
 * Computes sizes of given byte array field and adds a field with that size
 *
 * Config Paramters:
 *   - sourceField (String) : The field containing the byte array 
 *   - outputField (String) : The name of the field which will be added to store the size. Will overwrite value if field already 
 *                            exists
 */
public class ComputeFieldSize extends Stage {

  private final String sourceField;
  private final String outputField;

  public ComputeFieldSize(Config config) {
    super(config, new StageSpec().withRequiredProperties("sourceField", "outputField"));

    this.sourceField = config.getString("sourceField");
    this.outputField = config.getString("outputField");
  }

  /**
   * {@inheritDoc}
   * Here it processes document by computing and adding the correct field. 
   * @throws StageException If {@link ComputeFieldSize#sourceField} does not exist in document
   * @throws NullPointerException If {@link ComputeFieldSize#sourceField} is not a byte array in document
   */
  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (!doc.has(sourceField)) {
      throw new StageException(String.format("Document does not have source field: %s", sourceField));
    }

    doc.update(outputField, UpdateMode.OVERWRITE, doc.getBytes(sourceField).length);
    return null;
  }
}
