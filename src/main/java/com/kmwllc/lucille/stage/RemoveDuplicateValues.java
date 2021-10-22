package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

import java.util.Map.Entry;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This Stage removes duplicate values from the given list of fields.
 *
 * Config Parameters -
 *
 *   fieldMapping (Map<String, String>) : A mapping of fields to remove duplicates from and the field to output the result to.
 */
public class RemoveDuplicateValues extends Stage {

  private final Set<Entry<String, ConfigValue>> fieldMapping;

  public RemoveDuplicateValues(Config config) {
    super(config);
    this.fieldMapping = config.getConfig("fieldMapping").entrySet();
  }

  @Override
  public void start() throws StageException {
    if (fieldMapping.size() == 0)
      throw new StageException("Must supply at least one field to remove duplicate values from.");
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    for (Entry<String, ConfigValue> entry : fieldMapping) {
      String field = entry.getKey();

      if (!doc.has(field) || !doc.isMultiValued(field)) {
        continue;
      }

      List<String> uniqueValues = doc.getStringList(field).stream().distinct().collect(Collectors.toList());
      doc.update((String) entry.getValue().unwrapped(), UpdateMode.DEFAULT, uniqueValues.toArray(new String[0]));
    }

    return null;
  }
}
