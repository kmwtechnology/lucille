package com.kmwllc.lucille.stage;

import java.text.Normalizer;
import java.util.Iterator;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

/**
 * Removes diacritics and accents from String fields.
 * <br>
 * Config Parameters -
 * <br>
 * <p>
 * <b>source</b> (String) : Name of source field.
 * </p>
 * <p>
 * <b>destination</b> (String, Optional) : Name of field where transformed string is put. If not provided the string is mutated in place.
 * </p>
 */
public class RemoveDiacritics extends Stage {

  private final String source;
  private final String destination;

  public RemoveDiacritics(Config config) throws StageException {
    super(config, new StageSpec().withRequiredProperties("source").withOptionalProperties("destination"));
    this.source = config.getString("source");
    this.destination = ConfigUtils.getOrDefault(config, "destination", null);
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (!doc.has(source)) {
      return null;
    }

    String output = Normalizer.normalize(doc.getString(source), Normalizer.Form.NFKD).replaceAll("\\p{M}", "");

    if (destination == null) {
       doc.setField(source, output);
    } else {
       doc.setField(destination, output);
    }
    return null;
  }
}
