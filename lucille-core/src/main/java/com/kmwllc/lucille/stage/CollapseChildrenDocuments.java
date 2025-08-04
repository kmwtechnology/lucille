package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import java.util.Iterator;
import java.util.List;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

/**
 * <p> This stage will iterate over the children documents that are attached to the current document.
 * The fields to copy from the children document will be copied to multi-valued fields on the parent document.
 *
 * <p> If dropChildren is set to true, the children documents will be dropped from the
 * parent before letting the parent document continue down the pipeline.
 *
 * <p> Config Parameters:
 * <p> <b>fieldsToCopy</b> (List&lt;String&gt;): The fields you want to copy from a child document to the parent document.
 * <p> <b>dropChildren</b> (Boolean): Whether you want to drop the children Documents from their parents after processing.
 */
public class CollapseChildrenDocuments extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .requiredList("fieldsToCopy", new TypeReference<List<String>>(){})
      .requiredBoolean("dropChildren").build();

  private List<String> fieldsToCopy;
  private boolean dropChildren;

  public CollapseChildrenDocuments(Config config) { 
    super(config);
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
