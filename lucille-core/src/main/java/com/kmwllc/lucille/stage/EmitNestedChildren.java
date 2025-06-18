package com.kmwllc.lucille.stage;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.typesafe.config.Config;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This stage emits attached children documents, removing them from the parent document. Will skip document if no children are found.
 *
 * Config Parameters:
 * <p> - drop_parent (Boolean, Optional): if set to true, will mark parent document as dropped. Defaults to false
 * <p> - fields_to_copy (Map&lt;String, String&gt;, Optional): map of fields to copy from parent to children. It's a map of the source field name to the destination field name.
 * <p> - update_mode (String, Optional): The methodology by which you want to update fields, particularly for updating
 * multivalued fields on children documents. See {@link UpdateMode} for more information.
 */
public class EmitNestedChildren extends Stage {

  public static Spec SPEC = Spec.stage()
      .optParent("fields_to_copy", new TypeReference<Map<String, String>>() {})
      .withOptionalProperties("drop_parent", "update_mode");

  private final boolean dropParent;
  private final Map<String,Object> fieldsToCopy;
  private final UpdateMode updateMode;
  private static final Logger log = LoggerFactory.getLogger(EmitNestedChildren.class);

  public EmitNestedChildren(Config config) {
    super(config);

    this.dropParent = config.hasPath("drop_parent") ? config.getBoolean("drop_parent") : false;
    this.fieldsToCopy = config.hasPath("fields_to_copy") ? config.getConfig("fields_to_copy").root().unwrapped() : Map.of();
    this.updateMode = UpdateMode.fromConfig(config);
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (!doc.hasChildren()) {
      log.debug("document id: {} has no children. Skipping doc...", doc.getId());
      return null;
    }

    doc.setDropped(dropParent);
    List<Document> children = doc.getChildren();

    // copy fields from parent to children
    if (!fieldsToCopy.isEmpty()) {
      for (Document child : children) {
        for (String sourceField : fieldsToCopy.keySet()) {
          if (!doc.has(sourceField)) {
            log.debug("Field '{}' not found in parent document", sourceField);
            continue;
          }
          // handle multivalued fields if needed; treat values as Json to preserve types
          String destField = fieldsToCopy.get(sourceField).toString();
          if (doc.isMultiValued(sourceField)) {
            child.update(destField, updateMode, doc.getJsonList(sourceField).toArray(new JsonNode[0]));
          } else {
            child.update(destField, updateMode, doc.getJson(sourceField));
          }
        }
      }
    }

    doc.removeChildren();

    return children.iterator();
  }
}
