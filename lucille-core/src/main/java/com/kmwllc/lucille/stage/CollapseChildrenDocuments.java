package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Spec;
import java.util.Iterator;
import java.util.List;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

/**
 * This stage will iterate over the children documents that are attached to the currently 
 * processing document.  The fields to copy from the children document will be copied down
 * to multi-valued fields on the parent document.
 * 
 * If the dropChildren flag is set to true, the children documents will be dropped from the 
 * parent before letting the parent document continue down the pipeline.
 * 
 */
public class CollapseChildrenDocuments extends Stage {

  private List<String> fieldsToCopy;
  private boolean dropChildren;

  /**
   * Creates the CollapseChildrenDocuments stage from the given config.
   * @param config Configuration for the CollapseChildrenDocuments stage.
   */
  public CollapseChildrenDocuments(Config config) { 
    super(config, Spec.stage().withRequiredProperties("fieldsToCopy", "dropChildren"));
    fieldsToCopy = config.getStringList("fieldsToCopy"); 
    dropChildren = config.getBoolean("dropChildren"); 
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {

    if (doc.hasChildren()) {
      // We need to iterate the children and copy their field values to the parent.
      for (Document child : doc.getChildren()) {
        for (String fieldToCopy : fieldsToCopy) {
          if (child.has(fieldToCopy)) {
            doc.setOrAdd(fieldToCopy, child);
          }
        }
      }
      // drop the children if so configured to do so.
      if (dropChildren) {
        doc.removeChildren();
      }
    }
    return null;
  }

}
