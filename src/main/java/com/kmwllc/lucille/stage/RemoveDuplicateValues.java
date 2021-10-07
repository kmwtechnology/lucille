package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This Stage removes duplicate values from the given list of fields.
 *
 * Config Parameters -
 *
 *   fields (List<String>) : The list of fields to remove duplicates from
 */
public class RemoveDuplicateValues extends Stage {

  private final List<String> fields;

  public RemoveDuplicateValues(Config config) {
    super(config);
    this.fields = config.getStringList("fields");
  }

  @Override
  public void start() throws StageException {
    if (fields.size() == 0)
      throw new StageException("Must supply at least one field to remove duplicate values from.");
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    for (String field : fields) {
      if (!doc.has(field) || !doc.isMultiValued(field)) {
        continue;
      }

      Set<String> seenValues = new HashSet<>();
      List<String> values = doc.getStringList(field);
      for (int i = 0; i < values.size(); i++) {
        String value = values.get(i);

        if (!seenValues.add(value)) {
          doc.removeFromArray(field, i);
          i--;
          values = doc.getStringList(field);
        }
      }
    }

    return null;
  }
}
