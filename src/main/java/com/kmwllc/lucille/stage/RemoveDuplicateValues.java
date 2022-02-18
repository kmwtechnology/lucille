package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
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
 * This Stage removes duplicate values from the given list of fields.
 * <p>
 * Config Parameters -
 * <p>
 * fields (List<String>) : A list of fields to remove duplicates from.
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
      doc.removeDuplicateValues(field);
    }
    return null;
  }
}
