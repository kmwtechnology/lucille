package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.StageUtils;
import com.typesafe.config.Config;

import java.util.List;
import java.util.Set;

/**
 * Removes all occurrences of a given value from the source fields. Field values are not removed if
 * they contain a blacklisted value, only if it is an exact match between the two Strings.
 * Config Parameters:
 *
 *   - source (List<String>) : List of source field names.
 *   - values (List<String>) : The values to be blacklisted and removed from the source fields.
 */
public class DropValues extends Stage {

  private final List<String> sourceFields;
  private final List<String> values;

  public DropValues(Config config) {
    super(config);

    this.sourceFields = config.getStringList("source");
    this.values = config.getStringList("values");
  }

  @Override
  public void start() throws StageException {
    StageUtils.validateFieldNumNotZero(sourceFields, "Drop Values");
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    for (String source : sourceFields) {
      if (!doc.has(source))
        continue;

      List<String> fieldVals = doc.getStringList(source);
      doc.removeField(source);
      for (String value : fieldVals) {
        if (!values.contains(value)) {
          doc.addToField(source, value);
        }
      }
    }

    return null;
  }

  @Override
  public List<String> getPropertyList() {
    return List.of("source", "values");
  }
}
