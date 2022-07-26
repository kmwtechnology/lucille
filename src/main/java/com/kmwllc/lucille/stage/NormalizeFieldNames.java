package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.typesafe.config.Config;

import java.util.List;

/**
 * Normalizes a document's field values by replacing spaces and non-alphanumeric characters with given delimiters.
 * <p>
 * Config Parameters -
 * <ul>
 * <li>delimiter (String) : A delimiter to replace spaces, defaults to "_".</li>
 * <li>nonAlphanumReplacement (String) : A replacement for non-alphanumeric characters, defaults to "".</li>
 * </ul>
 */
public class NormalizeFieldNames extends Stage {

  private final String delimeter;
  private final String nonAlphanumReplacement;

  public NormalizeFieldNames(Config config) {
    super(config);
    this.delimeter = config.hasPath("delimeter") ? config.getString("delimeter") : "_";
    this.nonAlphanumReplacement = config.hasPath("nonAlphaNumReplacement") ? config.getString("nonAlphanumReplacement") : "";
  }

  @Override
  public List<JsonDocument> processDocument(JsonDocument doc) throws StageException {
    for (String field : doc.getFieldNames()) {
      if (JsonDocument.RESERVED_FIELDS.contains(field)) {
        continue;
      }

      String normalizedField = field.replaceAll(" ", delimeter).replaceAll("[^a-zA-Z0-9_.]", nonAlphanumReplacement);
      doc.renameField(field, normalizedField, UpdateMode.DEFAULT);
    }

    return null;
  }
}
