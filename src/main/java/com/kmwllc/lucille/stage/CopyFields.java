package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.StageUtils;
import com.typesafe.config.Config;

import java.util.ArrayList;
import java.util.List;

/**
 * This stage copies values from a given set of source fields to a given set of destination fields. If the same number
 * of fields are supplied for both sources and destinations, the fields will be copied from source_1 to dest_1 and
 * source_2 to dest_2. If either source or dest has only one field value, and the other has several, all of the
 * fields will be copied to/from the same field.
 *
 * Config Parameters:
 *
 *   - source (List<String>) : list of source field names
 *   - dest (List<String>) : list of destination field names. You can either supply the same number of source and destination fields
 *       for a 1-1 mapping of results or supply one destination field for all of the source fields to be mapped into.
 *   - mode (String) : Determines how the Copy Stage should behave if the destination field already exists: replace, skip
 */
public class CopyFields extends Stage {

  private final List<String> sourceFields;
  private final List<String> destFields;
  private final String mode;

  public CopyFields(Config config) {
    super(config);
    this.sourceFields = config.getStringList("source");
    this.destFields = config.getStringList("dest");
    this.mode  = config.getString("mode").toLowerCase();
  }

  @Override
  public void start() throws StageException {
    StageUtils.validateFieldNumNotZero(sourceFields, "Copy Fields");
    StageUtils.validateFieldNumNotZero(destFields, "Copy Fields");
    StageUtils.validateFieldNumsSeveralToOne(sourceFields, destFields, "Copy Fields");

    if (!mode.equals("replace") && !mode.equals("skip")) {
      throw new StageException("Invalid mode supplied to Copy Fields Stage. Must be 'replace' or 'skip'.");
    }
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    // If we are in replace mode, empty all of the destination fields first
    if (mode.equals("replace")) {
      for (String dest : destFields) {
        if (doc.has(dest)) {
          doc.removeField(dest);
        }
      }
    }

    for (int i = 0; i < sourceFields.size(); i++) {
      // If there is only one source or dest, use it. Otherwise, use the current source/dest.
      String sourceField = sourceFields.get(i);
      String destField = destFields.size() == 1 ? destFields.get(0) : destFields.get(i);

      if (!doc.has(sourceField))
        continue;

      // If we are in skip mode, skip this destination field if it already exists
      if (mode.equals("skip")) {
        if (doc.has(destField))
          continue;
      }

      for (String value : doc.getStringList(sourceField)) {
        doc.addToField(destField, value);
      }
    }

    return null;
  }
}
