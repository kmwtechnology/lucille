package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;

import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parses an array of floats from a string field and emits each number individually into a Lucille document field.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>field (String) : the name of the document field containing the JSON string to parse.</li>
 *   <li>dest (String, optional) : the name of the document field into which to write the floats.</li>
 * </ul>
 */
public class ParseFloats extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .requiredString("field")
      .optionalString("dest").build();

  private final String field;
  private final String dest;
  private static final Logger log = LoggerFactory.getLogger(ParseFloats.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TypeReference<List<Float>> TYPE_REFERENCE = new TypeReference<List<Float>>() {};

  public ParseFloats(Config config) {
    super(config);
    this.field = config.getString("field");
    this.dest = ConfigUtils.getOrDefault(config, "dest", null);
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
      doc.removeField(dest != null ? dest : field);
      for (Float d : floats) {
        doc.addToField(dest != null ? dest : field, d);
      }
    }
    return null;
  }
}
