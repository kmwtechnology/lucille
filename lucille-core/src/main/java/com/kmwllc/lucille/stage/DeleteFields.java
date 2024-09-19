package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.StageUtils;
import com.typesafe.config.Config;

import java.util.Iterator;
import java.util.List;

/**
 * Deletes a list of given fields from each document it processes.
 * <br>
 * Config Parameters -
 * <br>
 * fields (List&lt;String&gt;) : The list of fields to be deleted.
 */
public class DeleteFields extends Stage {

  private final List<String> fields;

  public DeleteFields(Config config) throws StageException {
    super(config, new StageSpec().withRequiredProperties("fields"));
    this.fields = config.getStringList("fields");
    StageUtils.validateListLenNotZero(fields, "DeleteFields");
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    for (String field : fields) {
      if (!doc.has(field)) {
        continue;
      }

      doc.removeField(field);
    }

    return null;
  }
}
