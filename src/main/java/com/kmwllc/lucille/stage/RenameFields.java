package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import java.util.List;

/**
 * This field renames a given set of source fields to a given set of destination fields. You must specify the same
 * number of source and destination fields.
 */
public class RenameFields extends Stage {

  private final List<String> sourceFields;
  private final List<String> destFields;

  public RenameFields(Config config) {
    super(config);
    sourceFields = config.getStringList("source");
    destFields = config.getStringList("dest");
  }

  @Override
  public void start() throws StageException {
    // Split the comma separated list of fields into a list of field names
    StageUtils.validateFieldNumNotZero(sourceFields, "Renaming Stage");
    StageUtils.validateFieldNumNotZero(destFields, "Renaming Stage");
    StageUtils.validateFieldNumsEqual(sourceFields, destFields, "Renaming Stage");
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    // For each field, if this document has the source field, rename it to the destination field
    for (int i = 0; i < sourceFields.size(); i++) {
      if (!doc.has(sourceFields.get(i)))
        continue;

      // Consider throwing an exception if destField exists already
      // TODO : Move to Document class
      for (String value : doc.getStringList(sourceFields.get(i))) {
        doc.addToField(destFields.get(i), value);
      }
      doc.removeField(sourceFields.get(i));
    }

    return null;
  }
}
