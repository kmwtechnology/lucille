package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.typesafe.config.Config;

import java.util.ArrayList;
import java.util.List;

/**
 * This Stage removes leading and trailing whitespace from every value in the given list of fields.
 *
 * Config Parameters -
 *
 *   fields (List<String>) : The list of fields to trim whitespace from
 */
public class TrimWhitespace extends Stage {

  private final List<String> fields;

  public TrimWhitespace(Config config) {
    super(config);
    this.fields = config.getStringList("fields");
  }

  @Override
  public void start() throws StageException {
    if (fields.size() == 0)
      throw new StageException("Must supply at least one field to trim whitespace from.");
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    for (String field : fields) {
      if (!doc.has(field))
        continue;

      List<String> values = new ArrayList<>();
      for (String value : doc.getStringList(field)) {
        values.add(value.trim());
      }

      doc.update(field, UpdateMode.OVERWRITE, values.toArray(new String[0]));
    }


    return null;
  }
}
