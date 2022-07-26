package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

import java.util.Map;
import java.util.Map.Entry;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Removes duplicate values from the given list of fields.
 * <br>
 * Config Parameters -
 * <br>
 * fieldMapping (Map<String, Object>) : A mapping of fields to remove duplicates from and the field to output the result to.
 */
public class RemoveDuplicateValues extends Stage {

  private final Map<String, Object> fieldMapping;

  public RemoveDuplicateValues(Config config) {
    super(config);
    this.fieldMapping = config.getConfig("fieldMapping").root().unwrapped();
  }

  @Override
  public void start() throws StageException {
    if (fieldMapping.size() == 0)
      throw new StageException("Must supply at least one field to remove duplicate values from.");
  }

  @Override
  public List<JsonDocument> processDocument(JsonDocument doc) throws StageException {
    for (String field : fieldMapping.keySet()) {
      if (!doc.has(field) || !doc.isMultiValued(field)) {
        continue;
      }
      String targetField = (String) fieldMapping.get(field);
      doc.removeDuplicateValues(field, targetField);
    }
    return null;
  }
}
