package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.util.StageUtils;
import com.typesafe.config.Config;

import java.util.Iterator;
import java.util.List;

/**
 * Removes all occurrences of a given value from the source fields. Field values are not removed if
 * they contain a blacklisted value, only if it is an exact match between the two Strings.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>source (List&lt;String&gt;) : List of source field names.</li>
 *   <li>values (List&lt;String&gt;) : The values to be blacklisted and removed from the source fields.</li>
 * </ul>
 */
public class DropValues extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .requiredList("source", new TypeReference<List<String>>(){})
      .requiredList("values", new TypeReference<List<String>>(){}).build();

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
  public Iterator<Document> processDocument(Document doc) throws StageException {
    for (String source : sourceFields) {
      if (!doc.has(source)) {
        continue;
      }

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
}
