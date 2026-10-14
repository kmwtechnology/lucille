package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import java.text.Normalizer;
import java.util.Iterator;
import java.util.regex.Pattern;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

/**
 * Removes diacritics and accents from String fields.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>source (String) : Name of source field.</li>
 *   <li>destination (String, Optional) : Name of field where transformed string is put. If not provided the string is mutated in place.</li>
 * </ul>
 */
public class RemoveDiacritics extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .requiredString("source")
      .optionalString("destination").build();

  private static final Pattern MARKS = Pattern.compile("\\p{M}");

  private final String source;
  private final String destination;

  public RemoveDiacritics(Config config) throws StageException {
    super(config);
    this.source = config.getString("source");
    this.destination = ConfigUtils.getOrDefault(config, "destination", null);
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (!doc.has(source)) {
      return null;
    }

    String output = MARKS.matcher(Normalizer.normalize(doc.getString(source), Normalizer.Form.NFKD)).replaceAll("");

    if (destination == null) {
       doc.setField(source, output);
    } else {
       doc.setField(destination, output);
    }
    return null;
  }
}
