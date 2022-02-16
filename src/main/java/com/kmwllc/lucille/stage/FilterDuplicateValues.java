package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.typesafe.config.Config;

import java.util.*;

/**
 * A stage to filter out duplicate values on multivalued fields.
 */
public class FilterDuplicateValues extends Stage {

  public FilterDuplicateValues(Config config) {
    super(config);
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    Set<String> fieldNames = doc.getFieldNames();
    for (String fieldName : fieldNames) {

      if (doc.isMultiValued(fieldName)) {
        List<String> values = doc.getStringList(fieldName);

        // LinkedHashSet to preserve order
        Set<String> set = new LinkedHashSet<>();
        for (String value : values) {
          if (!set.contains(value)) {
            set.add(value);
          }
        }

        // use object
        String[] updatedValues = set.toArray(new String[0]);
        doc.update(fieldName, UpdateMode.OVERWRITE, updatedValues);
      }
    }
    return null;
  }
}
