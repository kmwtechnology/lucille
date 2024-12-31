package com.kmwllc.lucille.stage;


import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.typesafe.config.Config;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This stage emits attached children documents, removing them from the parent document. Will skip document if no children is found.
 *
 * Config Parameters:
 * - drop_parent (Boolean, Optional): if set to true, will mark parent document as dropped. Defaults to false
 * - fields_to_copy (List<String>, Optional): list of fields to copy from parent to children
 */
public class EmitNestedChildren extends Stage {

  private final boolean dropParent;
  private final List<String> fieldsToCopy;
  private final UpdateMode updateMode;
  private static final Logger log = LoggerFactory.getLogger(EmitNestedChildren.class);

  public EmitNestedChildren(Config config) {
    super(config, new StageSpec().withOptionalProperties("drop_parent", "fields_to_copy", "update_mode"));
    this.dropParent = config.hasPath("drop_parent") ? config.getBoolean("drop_parent") : false;
    this.fieldsToCopy = config.hasPath("fields_to_copy") ? config.getStringList("fields_to_copy") : List.of();
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
        for (String field : fieldsToCopy) {
          if (!doc.has(field)) {
            log.warn("Field '{}' not found in parent document", field);
            continue;
          }
          // handle multivalued fields if needed, we want to be a list if multivalued
          if (doc.isMultiValued(field)) {
            child.update(field, updateMode, doc.getJsonList(field).toArray(new com.fasterxml.jackson.databind.JsonNode[0]));
          } else {
            child.update(field, updateMode, doc.getJson(field));
          }
        }
      }
    }

    doc.removeChildren();

    return children.iterator();
  }
}
