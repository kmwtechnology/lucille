package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;


import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * This stage parses a JSON string and sets fields on the processed document according to the configured mapping using
 * JSONPointers.
 * @see <a href="https://datatracker.ietf.org/doc/html/rfc6901">JSONPointers</a>
 * Config Parameters
 * <p>
 *   - src (String) : The field containing the JSON string to be parsed.
 *   - jsonFieldPointers (Map<String, Object>) : Defines the mapping between JSON pointers to fields in the parsed JSON
 *   and the destination fields in the processed document.
 * </p>
 */
public class ParseJson extends Stage {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String src;
  private final Map<String, Object> jsonFieldPointers;

  public ParseJson(Config config) {
    super(config);
    this.src = config.getString("src");
    this.jsonFieldPointers = config.getConfig("jsonFieldPointers").root().unwrapped();
  }

  /**
   *
   * @param doc
   * @return
   * @throws StageException when the src JSON string cannot be parsed.
   */
  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    try {
      //Should this be a byte[]? Document doesn't support byte fields currently.
      JsonNode srcNode = MAPPER.readTree(doc.getString(this.src));
      for (Entry<String, Object> entry : this.jsonFieldPointers.entrySet()) {
        JsonNode val = srcNode.at((String) entry.getValue());
        doc.setField(entry.getKey(), val);
      }
      doc.removeField(this.src);
      return null;
    } catch (JsonProcessingException e) {
      throw new StageException("Not able to parse JSON for document", e);
    }
  }
}
