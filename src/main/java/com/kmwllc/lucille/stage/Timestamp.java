package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * dest_field (String) : The field to place the timestamp into.
 */
public class Timestamp extends Stage {

  private final String destField;

  public Timestamp(Config config) {
    super(config);
    this.destField = config.getString("dest_field");
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    Instant now = Instant.now();
    String dateStr = DateTimeFormatter.ISO_INSTANT.format(now);
    doc.setField(destField, dateStr);

    return null;
  }
}
