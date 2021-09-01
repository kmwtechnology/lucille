package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.StageUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * This field renames a given set of source fields to a given set of destination fields. You must specify the same
 * number of source and destination fields.
 *
 * Config Parameters:
 *
 *   - field_mapping (Map<String, String>) : A 1-1 mapping of original field names to new field names.
 */
public class RenameFields extends Stage {

  private final List<String> sourceFields;
  private final List<String> destFields;

  public RenameFields(Config config) {
    super(config);

    sourceFields = new ArrayList<>();
    destFields = new ArrayList<>();
    Set<Entry<String, ConfigValue>> values = config.getConfig("field_mapping").entrySet();
    for (Entry<String, ConfigValue> entry : values) {
      sourceFields.add(entry.getKey());
      destFields.add((String) entry.getValue().unwrapped());
    }
  }

  @Override
  public void start() throws StageException {
    // Split the comma separated list of fields into a list of field names
    StageUtils.validateFieldNumNotZero(sourceFields, "RenameFields");
    StageUtils.validateFieldNumNotZero(destFields, "RenameFields");
    StageUtils.validateFieldNumsEqual(sourceFields, destFields, "RenameFields");
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
