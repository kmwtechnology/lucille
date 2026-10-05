package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;

import java.util.Iterator;
import java.util.regex.Pattern;

/**
 * Normalizes a document's field values by replacing spaces and non-alphanumeric characters with given delimiters.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>delimiter (String) : A delimiter to replace spaces, defaults to "_".</li>
 *   <li>nonAlphanumReplacement (String) : A replacement for non-alphanumeric characters, defaults to "".</li>
 * </ul>
 */
public class NormalizeFieldNames extends Stage {

  public static final Spec SPEC = SpecBuilder.stage().optionalString("delimiter", "nonAlphanumReplacement").build();

  private static final Pattern SPACE = Pattern.compile(" ");
  private static final Pattern NON_ALPHANUM = Pattern.compile("[^a-zA-Z0-9_.]");

  private final String delimeter;
  private final String nonAlphanumReplacement;

  public NormalizeFieldNames(Config config) {
    super(config);
    this.delimeter = config.hasPath("delimeter") ? config.getString("delimeter") : "_";
    this.nonAlphanumReplacement = config.hasPath("nonAlphaNumReplacement") ? config.getString("nonAlphanumReplacement") : "";
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    for (String field : doc.getFieldNames()) {
      if (Document.RESERVED_FIELDS.contains(field)) {
        continue;
      }

      String spaced = SPACE.matcher(field).replaceAll(delimeter);
      String normalizedField = NON_ALPHANUM.matcher(spaced).replaceAll(nonAlphanumReplacement);
      doc.renameField(field, normalizedField, UpdateMode.DEFAULT);
    }

    return null;
  }
}
