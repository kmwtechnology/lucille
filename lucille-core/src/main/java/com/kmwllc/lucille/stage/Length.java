package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;

import com.kmwllc.lucille.core.spec.SpecBuilder;
import java.util.Iterator;
import java.util.Map.Entry;
import com.typesafe.config.Config;

import java.util.Map;

/**
 * Determines the length of a field and places the value into a specified field.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>fieldMapping (Map&lt;String, String&gt;) : A mapping of the field to check the size of to the name of the field to place the length into.</li>
 * </ul>
 */
public class Length extends Stage {

  public static final Spec SPEC = SpecBuilder.stage().requiredParent("fieldMapping", new TypeReference<Map<String, String>>() {})
      .build();

  private final Map<String, Object> fieldMap;

  public Length(Config config) {
    super(config);

    this.fieldMap = config.getConfig("fieldMapping").root().unwrapped();
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    for (Entry<String, Object> e : fieldMap.entrySet()) {
      doc.setField((String) e.getValue(), doc.length(e.getKey()));
    }

    return null;
  }
}
