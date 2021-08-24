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
public class RenamingStage extends Stage {

  private final String SOURCE_FIELDS_STR;
  private final String DEST_FIELDS_STR;

  public RenamingStage(Config config) {
    super(config);
    SOURCE_FIELDS_STR = config.getString("source");
    DEST_FIELDS_STR = config.getString("dest");
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    // Split the comma separated list of fields into a list of field names
    String[] srcFields = SOURCE_FIELDS_STR.split(",");
    String[] destFields = DEST_FIELDS_STR.split(",");

    StageUtil.validateFieldNumNotZero(SOURCE_FIELDS_STR, "Renaming Stage");
    StageUtil.validateFieldNumNotZero(DEST_FIELDS_STR, "Renaming Stage");
    StageUtil.validateFieldNumsEqual(SOURCE_FIELDS_STR, DEST_FIELDS_STR, "Renaming Stage");

    // For each field, if this document has the source field, rename it to the destination field
    for (int i = 0; i < srcFields.length; i++) {
      if (!doc.has(srcFields[i]))
        continue;

      // Consider throwing an exception if destField exists already
      doc.addToField(destFields[i], doc.getString(srcFields[i]));
      doc.removeField(srcFields[i]);
    }

    return null;
  }
}
