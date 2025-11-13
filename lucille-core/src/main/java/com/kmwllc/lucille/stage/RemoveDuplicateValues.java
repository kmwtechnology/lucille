package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;

import java.util.Iterator;
import java.util.Map;

/**
 * Removes duplicate values from the given list of fields.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>fieldMapping (Map&lt;String, Object&gt;) : A mapping of fields to remove duplicates from and the field to output the result to.</li>
 * </ul>
 */
public class RemoveDuplicateValues extends Stage {

  public static final Spec SPEC = SpecBuilder.stage().requiredParent("fieldMapping", new TypeReference<Map<String, Object>>() {})
      .build();

  private final Map<String, Object> fieldMapping;

  public RemoveDuplicateValues(Config config) {
    super(config);
    this.fieldMapping = config.getConfig("fieldMapping").root().unwrapped();
  }

  @Override
  public void start() throws StageException {
    if (fieldMapping.size() == 0) {
      throw new StageException("Must supply at least one field to remove duplicate values from.");
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
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
