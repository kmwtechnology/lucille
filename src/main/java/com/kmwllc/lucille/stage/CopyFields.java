package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.StageUtils;
import com.kmwllc.lucille.core.UpdateMode;
import com.typesafe.config.Config;

import java.util.List;

/**
 * Copies values from a given set of source fields to a given set of destination fields. If the same number
 * of fields are supplied for both sources and destinations, the fields will be copied from source_1 to dest_1 and
 * source_2 to dest_2. If either source or dest has only one field value, and the other has several, all of the
 * fields will be copied to/from the same field.
 *
 * Config Parameters:
 *
 *   - source (List<String>) : list of source field names
 *   - dest (List<String>) : list of destination field names. You can either supply the same number of source and destination fields
 *       for a 1-1 mapping of results or supply one destination field for all of the source fields to be mapped into.
 *   - update_mode (String, Optional) : Determines how writing will be handling if the destination field is already populated.
 *      Can be 'overwrite', 'append' or 'skip'. Defaults to 'overwrite'.
 */
public class CopyFields extends Stage {

  private final List<String> sourceFields;
  private final List<String> destFields;
  private final UpdateMode updateMode;

  public CopyFields(Config config) {
    super(config, new StageSpec()
            .withRequiredProperties("source", "dest")
            .withOptionalProperties("update_mode"));
    this.sourceFields = config.getStringList("source");
    this.destFields = config.getStringList("dest");
    this.updateMode = UpdateMode.fromConfig(config);
  }

  @Override
  public void start() throws StageException {
    StageUtils.validateFieldNumNotZero(sourceFields, "Copy Fields");
    StageUtils.validateFieldNumNotZero(destFields, "Copy Fields");
    StageUtils.validateFieldNumsSeveralToOne(sourceFields, destFields, "Copy Fields");
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    for (int i = 0; i < sourceFields.size(); i++) {
      // If there is only one source or dest, use it. Otherwise, use the current source/dest.
      String sourceField = sourceFields.get(i);
      String destField = destFields.size() == 1 ? destFields.get(0) : destFields.get(i);

      if (!doc.has(sourceField))
        continue;

      doc.update(destField, updateMode, doc.getStringList(sourceField).toArray(new String[0]));
    }

    return null;
  }
}
