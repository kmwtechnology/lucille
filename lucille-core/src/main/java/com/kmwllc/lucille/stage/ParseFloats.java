package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import java.util.Iterator;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ParseFloats extends Stage {

  private final String field;
  private static final Logger log = LogManager.getLogger(ParseFloats.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TypeReference<List<Float>> TYPE_REFERENCE = new TypeReference<List<Float>>() {};

  public ParseFloats(Config config) {
    super(config, new StageSpec().withRequiredProperties("field"));
    this.field = config.getString("field");
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (doc.has(field)) {
      String value = doc.getString(this.field);
      List<Float> floats;
      try {
        floats = MAPPER.readValue(value, TYPE_REFERENCE);
      } catch (Exception e) {
        log.warn("String: {} cannot be parsed.", value, e);
        return null;
      }
      doc.removeField(this.field);
      for (Float d : floats) {
        doc.addToField(this.field, d);
      }
    }
    return null;
  }
}
