package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.typesafe.config.Config;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
    super(config, new StageSpec().withOptionalProperties("delimiter", "nonAlphanumReplacement"));
    this.delimeter = config.hasPath("delimeter") ? config.getString("delimeter") : "_";
    this.nonAlphanumReplacement = config.hasPath("nonAlphaNumReplacement") ? config.getString("nonAlphanumReplacement") : "";
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    Set<String> fieldNames = new HashSet(doc.getFieldNames());
    for (String field : fieldNames) {
      if (Document.RESERVED_FIELDS.contains(field)) {
        continue;
      }

      String normalizedField = field.replaceAll(" ", delimeter).replaceAll("[^a-zA-Z0-9_.]", nonAlphanumReplacement);
      doc.renameField(field, normalizedField, UpdateMode.DEFAULT);
    }

    return null;
  }
}
