package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.StageUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

import java.util.*;
import java.util.Map.Entry;

/**
 * This field renames a given set of source fields to a given set of destination fields. You must specify the same
 * number of source and destination fields.
 *
 * Config Parameters:
 *
 *   - field_mapping (Map<String, String>) : A 1-1 mapping of original field names to new field names.
 */
public class RenameFields extends Stage {

  private final Set<Entry<String, ConfigValue>> fieldMap;

  public RenameFields (Config config) {
    super(config);


    this.fieldMap = config.getConfig("field_mapping").entrySet();
  }

  @Override
  public void start() throws StageException {
    if (fieldMap.size() == 0)
      throw new StageException("field_mapping must have at least one source-dest pair for Rename Fields");
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    // For each field, if this document has the source field, rename it to the destination field
    for (Entry<String, ConfigValue> fieldPair : fieldMap) {
      if (!doc.has(fieldPair.getKey()))
        continue;
      
      // TODO : Determine what to do if the destination field already exists
      String dest = (String) fieldPair.getValue().unwrapped();
      doc.renameField(fieldPair.getKey(), dest);
    }

    return null;
  }
}
