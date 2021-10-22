package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import java.util.List;

/**
 * This Stage deletes a list of given fields from each Document it processes
 *
 * Config Parameters -
 *
 *   fields (List<String>) : The list of fields to be deleted
 */
public class DeleteFields extends Stage {

  private final List<String> fields;

  public DeleteFields(Config config) {
    super(config);

    this.fields = config.getStringList("fields");
  }

  @Override
  public void start() throws StageException {
    if (fields.size() == 0)
      throw new StageException("Must supply at least one field to be deleted.");
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    for (String field : fields) {
      if (!doc.has(field))
        continue;

      doc.removeField(field);
    }

    return null;
  }
}
