package com.kmwllc.lucille.stage;


import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This stage emits attached children documents, removing them from the parent document. Will skip document if no children is found.
 *
 * Config Parameters:
 * - drop_parent (Boolean): if set to true, will mark parent document as dropped
 *
 */
public class EmitNestedChildren extends Stage {

  private final boolean dropParent;
  private static final Logger log = LoggerFactory.getLogger(EmitNestedChildren.class);

  public EmitNestedChildren(Config config) {
    super(config, new StageSpec().withRequiredProperties("drop_parent"));
    this.dropParent = config.getBoolean("drop_parent");
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (!doc.hasChildren()) {
      log.info("document id: {} has no children. Skipping doc...", doc.getId());
      return null;
    }

    doc.setDropped(dropParent);
    List<Document> children = doc.getChildren();
    doc.removeChildren();

    return children.iterator();
  }
}
