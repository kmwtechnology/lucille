package com.kmwllc.lucille.stage;

import java.util.Iterator;
import java.util.List;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

public class CollapseChildrenDocuments extends Stage {

  private List<String> fieldsToCopy;
  private boolean dropChildren;
  
  public CollapseChildrenDocuments(Config config) { 
    super(config, new StageSpec().withRequiredProperties("fieldsToCopy", "dropChildren"));
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
            doc.addToField(fieldToCopy, child.getString(fieldToCopy));
          }
        }
      }
      // drop the children,.
      if (dropChildren) {
        doc.removeChildren();
      }
    }
    return null;
  }

}
