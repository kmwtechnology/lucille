package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.typesafe.config.Config;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Renames a given set of source fields to a given set of destination fields. You must specify the same
 * number of source and destination fields.
 *
 * Config Parameters:
 *
 *   - fieldMapping (Map&lt;String, String&gt;) : A 1-1 mapping of original field names to new field names.
 *   - update_mode (String, Optional) : Determines how writing will be handling if the destination field is already populated.
 *       Can be 'overwrite', 'append' or 'skip'. Defaults to 'overwrite'.
 */
public class RenameFields extends Stage {

  private final Map<String, Object> fieldMap;
  private final UpdateMode updateMode;

  public RenameFields(Config config) {
    super(config, Spec.stage()
        .withOptionalProperties("update_mode")
        .reqParentName("fieldMapping"));

    this.fieldMap = config.getConfig("fieldMapping").root().unwrapped();
    this.updateMode = UpdateMode.fromConfig(config);
  }

  /**
   *
   * @throws StageException if the field mapping is empty.
   */
  @Override
  public void start() throws StageException {
    if (fieldMap.size() == 0) {
      throw new StageException("field_mapping must have at least one source-dest pair for Rename Fields");
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    // For each field, if this document has the source field, rename it to the destination field
    for (Entry<String, Object> fieldPair : fieldMap.entrySet()) {
      if (!doc.has(fieldPair.getKey())) {
        continue;
      }

      String dest = (String) fieldPair.getValue();
      doc.renameField(fieldPair.getKey(), dest, updateMode);
    }

    return null;
  }
}
