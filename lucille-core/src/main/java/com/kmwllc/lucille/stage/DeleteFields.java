package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Spec;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import java.util.Iterator;
import java.util.List;

/**
 * Deletes a list of given fields from each document it processes.
 * <br>
 * Config Parameters:
 * <p> fields (List&lt;String&gt;) : The list of fields to be deleted.
 */
public class DeleteFields extends Stage {

  private final List<String> fields;

  /**
   * Creates the DeleteFields stage from the given config.
   * @param config Configuration for the DeleteFields stage.
   */
  public DeleteFields(Config config) {
    super(config, Spec.stage().withRequiredProperties("fields"));
    this.fields = config.getStringList("fields");
  }

  @Override
  public void start() throws StageException {
    if (fields.size() == 0) {
      throw new StageException("Must supply at least one field to be deleted.");
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    for (String field : fields) {
      if (!doc.has(field)) {
        continue;
      }

      doc.removeField(field);
    }

    return null;
  }
}
