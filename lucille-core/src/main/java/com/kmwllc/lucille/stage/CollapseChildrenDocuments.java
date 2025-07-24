package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Spec;
import java.util.Iterator;
import java.util.List;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

/**
 * This stage will iterate over the children documents that are attached to the current document. The fields to copy from the
 * children document will be copied to multi-valued fields on the parent document. If dropChildren is set to true, the children
 * documents will be dropped from the parent before letting the parent document continue down the pipeline.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>fieldsToCopy (List&lt;String&gt;) : The fields you want to copy from a child document to the parent document.</li>
 *   <li>dropChildren (Boolean) : Whether you want to drop the children Documents from their parents after processing.</li>
 * </ul>
 */
public class CollapseChildrenDocuments extends Stage {

  private List<String> fieldsToCopy;
  private boolean dropChildren;

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
