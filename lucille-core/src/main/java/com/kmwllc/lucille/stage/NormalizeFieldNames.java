package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.ConfigUtils;
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
 * Normalizes a document's field names by replacing spaces and non-alphanumeric characters with given delimiters.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>delimiter (String) : A delimiter to replace spaces, defaults to "_".</li>
 *   <li>nonAlphanumericReplacement (String) : A replacement for non-alphanumeric characters, defaults to "".</li>
 * </ul>
 */
public class NormalizeFieldNames extends Stage {

  public static final Spec SPEC = SpecBuilder.stage().optionalString("delimiter", "nonAlphanumericReplacement").build();

  private static final Pattern SPACE = Pattern.compile(" ");
  private static final Pattern NON_ALPHANUM = Pattern.compile("[^a-zA-Z0-9_.]");

  private final String delimiter;
  private final String nonAlphanumericReplacement;

  public NormalizeFieldNames(Config config) {
    super(config);
    this.delimiter = ConfigUtils.getOrDefault(config, "delimiter", "_");
    this.nonAlphanumericReplacement = ConfigUtils.getOrDefault(config, "nonAlphanumericReplacement", "");
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    for (String field : doc.getFieldNames()) {
      if (Document.RESERVED_FIELDS.contains(field)) {
        continue;
      }

      String spaced = SPACE.matcher(field).replaceAll(delimiter);
      String normalizedField = NON_ALPHANUM.matcher(spaced).replaceAll(nonAlphanumericReplacement);
      doc.renameField(field, normalizedField, UpdateMode.DEFAULT);
    }

    return null;
  }
}
