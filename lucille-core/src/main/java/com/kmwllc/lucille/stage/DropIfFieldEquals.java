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
 * Stage also gives an option to invert the match, so that the document is dropped if the value does not match.
 */
public class DropIfFieldEquals extends Stage {

  private final String field;
  private final String value;
  private final boolean caseSensitive;
  private final boolean inverted;

  public DropIfFieldEquals(Config config) {
    super(config, new StageSpec()
        .withRequiredProperties("field", "value")
        .withOptionalProperties("caseSensitive", "inverted")
    );

    // required
    field = config.getString("field");
    value = config.getString("value");

    // optional
    caseSensitive = ConfigUtils.getOrDefault(config, "caseSensitive", false);
    inverted = ConfigUtils.getOrDefault(config, "inverted", false);
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {

    if (!doc.has(field)) {
      return null;
    }

    String docValue = doc.getString(field);
    boolean match = caseSensitive ? docValue.equals(value) : docValue.equalsIgnoreCase(value);

    // drop document if match and not inverted or no match and inverted
    if (match ^ inverted) {
      doc.setDropped(true);
    }

    return null;
  }
}
