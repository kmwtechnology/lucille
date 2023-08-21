package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import java.util.Iterator;

/**
 * This stage will drop a document if it contains a given field and the value of that field matches a given value.
 * Stage gives an option to do a case-sensitive comparison but is not case-sensitive by default.
 */
public class DropIfFieldEquals extends Stage {

  private final String field;
  private final String value;
  private final boolean caseSensitive;

  public DropIfFieldEquals(Config config) {
    super(config, new StageSpec()
        .withRequiredProperties("field", "value")
        .withOptionalProperties("caseSensitive")
    );

    field = config.getString("field");
    value = config.getString("value");
    caseSensitive = ConfigUtils.getOrDefault(config, "caseSensitive", false);
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {

    if (!doc.has(field)) {
      return null;
    }

    String docValue = doc.getString(field);
    if (caseSensitive) {
      if (docValue.equals(value)) {
        doc.setDropped(true);
      }
    } else {
      if (docValue.equalsIgnoreCase(value)) {
        doc.setDropped(true);
      }
    }

    return null;
  }
}
