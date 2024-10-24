package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;

/**
 * Adds a timestamp into a given field.
 * <p>
 * Config Parameters -
 * <ul>
 * <li>destField (String) : The field to place the timestamp into.</li>
 * </ul>
 */
public class Timestamp extends Stage {

  private final String destField;

  public Timestamp(Config config) {
    super(config, new StageSpec().withRequiredProperties("destField"));
    this.destField = config.getString("destField");
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    Instant now = Instant.now();
    String dateStr = DateTimeFormatter.ISO_INSTANT.format(now);
    doc.setField(destField, dateStr);

    return null;
  }
}
