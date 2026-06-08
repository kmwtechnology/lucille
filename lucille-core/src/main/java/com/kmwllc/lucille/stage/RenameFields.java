package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Renames a given set of source fields to a given set of destination fields. You must specify the same
 * number of source and destination fields.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>fieldMapping (Map&lt;String, String&gt;) : A 1-1 mapping of original field names to new field names.</li>
 *   <li>updateMode (String, Optional) : Determines how writing will be handling if the destination field is already populated. Can be
 *   'overwrite', 'append' or 'skip'. Defaults to 'overwrite'.</li>
 *   <li>applyToChildren (Boolean, Optional) : Determines whether renaming of fields will be applied to attached child docs (the stage
 *   is automatically applied to emitted children). Applies to all attached children, irrespective of stage conditions.</li>
 * </ul>
 */
public class RenameFields extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .optionalString("updateMode")
      .optionalBoolean("applyToChildren")
      .requiredParent("fieldMapping", new TypeReference<Map<String, String>>() {}).build();

  private final Map<String, Object> fieldMap;
  private final UpdateMode updateMode;
  private final boolean applyToChildren;

  public RenameFields(Config config) {
    super(config);

    this.fieldMap = config.getConfig("fieldMapping").root().unwrapped();
    this.updateMode = UpdateMode.fromConfig(config);
    this.applyToChildren = ConfigUtils.getOrDefault(config, "applyToChildren", false);
  }

  /**
   *
   * @throws StageException if the field mapping is empty.
   */
  @Override
  public void start() throws StageException {
    if (fieldMap.size() == 0) {
      throw new StageException("fieldMapping must have at least one source-dest pair for Rename Fields");
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

    if (applyToChildren) {
      List<Document> children = doc.getChildren();
      if (!children.isEmpty()) {
        for (Document childDoc : children) {
          processDocument(childDoc);
        }

        doc.removeChildren();

        for (Document childDoc : children) {
          doc.addChild(childDoc);
        }
      }
    }

    return null;
  }
}
