package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.*;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * This stage parses a JSON string and sets fields on the processed document according to the configured mapping using
 * JsonPath expressions.
 *
 * @see <a href="https://github.com/json-path/JsonPath">JsonPath</a>
 * Config Parameters
 * <p>
 * - src (String) : The field containing the JSON string to be parsed.
 * - jsonFieldPaths (Map<String, Object>) : Defines the mapping from JsonPath expressions
 * to the destination fields in the processed document.
 * </p>
 */
public class ParseJson extends Stage {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String src;
  private final Map<String, Object> jsonFieldPaths;
  private final Configuration jsonPathConf;
  private ParseContext jsonParseCtx;

  public ParseJson(Config config) {
    super(config);
    this.src = config.getString("src");
    this.jsonFieldPaths = config.getConfig("jsonFieldPointers").root().unwrapped();
    this.jsonPathConf = Configuration.builder()
      .jsonProvider(new JacksonJsonProvider())
      .mappingProvider(new JacksonMappingProvider())
      .options(Option.DEFAULT_PATH_LEAF_TO_NULL, Option.SUPPRESS_EXCEPTIONS).build();
  }

  @Override
  public void start() {
    this.jsonParseCtx = JsonPath.using(this.jsonPathConf);
  }

  /**
   * @param doc
   * @return
   */
  @Override
  public List<Document> processDocument(Document doc) throws StageException {

    DocumentContext ctx = this.jsonParseCtx.parse(doc.getString(this.src));
    for (Entry<String, Object> entry : this.jsonFieldPaths.entrySet()) {
      //JsonNode val = srcNode.at((String) entry.getValue());
      JsonNode val = ctx.read((String) entry.getValue(), JsonNode.class);

      doc.setField(entry.getKey(), val);
    }
    doc.removeField(this.src);
    return null;
  }
}
