package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import java.util.List;

/**
 * This stage copies values from a given set of source fields to a given set of destination fields. If the same number
 * of fields are supplied for both sources and destinations, the fields will be copied from source_1 to dest_1 and
 * source_2 to dest_2. If either source or dest has only one field value, and the other has several, all of the
 * fields will be copied to/from the same field.
 */
public class CopyingStage extends Stage {

  private final String SOURCE_FIELDS_STR;
  private final String DEST_FIELDS_STR;

  public CopyingStage(Config config) {
    super(config);
    this.SOURCE_FIELDS_STR = config.getString("source");
    this.DEST_FIELDS_STR = config.getString("dest");
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    String[] srcFields = SOURCE_FIELDS_STR.split(",");
    String[] destFields = DEST_FIELDS_STR.split(",");

    StageUtil.validateFieldNumNotZero(SOURCE_FIELDS_STR, "Copying Stage");
    StageUtil.validateFieldNumNotZero(DEST_FIELDS_STR, "Copying Stage");
    StageUtil.validateFieldNumsOneToSeveral(SOURCE_FIELDS_STR, DEST_FIELDS_STR, "Copying Stage");

    int numFields = Integer.max(destFields.length, srcFields.length);

    for (int i = 0; i < numFields; i++) {
      // If there is only one source or dest, use it. Otherwise, use the current source/dest.
      String sourceField = srcFields.length == 1 ? srcFields[0] : srcFields[i];
      String destField = destFields.length == 1 ? destFields[0] : destFields[i];

      if (doc.has(sourceField))
        continue;

      doc.setField(destField, doc.getString(sourceField));
    }

    return null;
  }
}
