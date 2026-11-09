package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import java.text.Normalizer;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.kmwllc.lucille.util.StageUtils;
import com.typesafe.config.Config;

/**
 * Removes diacritics and accents from every value of a String field.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>source (String) : Name of source field.</li>
 *   <li>dest (String, Optional) : Name of field where transformed string is put. If not provided the string is mutated in place.</li>
 * </ul>
 */
public class RemoveDiacritics extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .requiredString("source")
      .optionalString("dest").build();

  private static final Pattern MARKS = Pattern.compile("\\p{M}");

  private final String source;
  private final String destination;

  public RemoveDiacritics(Config config) throws StageException {
    super(config);
    this.source = config.getString("source");
    this.destination = ConfigUtils.getStringOrDefault(config, "dest", null);
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    List<String> values = StageUtils.getNonNullStrings(doc, source);
    if (values.isEmpty()) {
      return null;
    }

    String[] outputs = values.stream()
        .map(value -> MARKS.matcher(Normalizer.normalize(value, Normalizer.Form.NFKD)).replaceAll(""))
        .toArray(String[]::new);
    doc.update(destination == null ? source : destination, UpdateMode.OVERWRITE, outputs);
    return null;
  }
}
