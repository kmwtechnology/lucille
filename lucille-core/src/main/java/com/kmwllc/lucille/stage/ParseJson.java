package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.databind.JsonNode;
import com.jayway.jsonpath.*;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.kmwllc.lucille.core.Spec;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import nl.altindag.ssl.util.internal.StringUtils;

/**
 * Parses a JSON string and sets fields on the processed document according to the configured mapping using
 * JsonPath expressions.
 *
 * @see <a href="https://github.com/json-path/JsonPath">JsonPath</a>
 * Config Parameters
 * <p>
 * - src (String) : The field containing the JSON string to be parsed.
 * - sourceIsBase64: When set to true, indicates that the source field is base64 encoded. In this case the stage will decode
 * the field value before parsing.
 * - jsonFieldPaths (Map&lt;String, Object&gt;) : Defines the mapping from JsonPath expressions
 * to the destination fields in the processed document.
 * </p>
 */
public class ParseJson extends Stage {
  private static final Base64.Decoder DECODER = Base64.getDecoder();

  private final String src;
  private final Map<String, Object> jsonFieldPaths;
  private final Configuration jsonPathConf;
  private final boolean sourceIsBase64;

  private ParseContext jsonParseCtx;

  public ParseJson(Config config) {
    super(config, Spec.stage()
        .withRequiredProperties("src")
        .withOptionalProperties("sourceIsBase64")
        .withRequiredParents("jsonFieldPaths"));
    this.src = config.getString("src");
    this.jsonFieldPaths = config.getConfig("jsonFieldPaths").root().unwrapped();
    this.sourceIsBase64 = config.hasPath("sourceIsBase64") && config.getBoolean("sourceIsBase64");
    this.jsonPathConf = Configuration.builder()
        .jsonProvider(new JacksonJsonProvider())
        .mappingProvider(new JacksonMappingProvider())
        .options(Option.DEFAULT_PATH_LEAF_TO_NULL, Option.SUPPRESS_EXCEPTIONS).build();
  }

  @Override
  public void start() throws StageException {
    this.jsonParseCtx = JsonPath.using(this.jsonPathConf);

    for (Entry<String, Object> entry : this.jsonFieldPaths.entrySet()) {
      if (!isValidEntry(entry)) {
        throw new StageException("jsonFieldPaths mapping contains a blank or null key/value.");
      }
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    DocumentContext ctx;
    if (this.sourceIsBase64) {
      try (InputStream stream = new ByteArrayInputStream(DECODER.decode(doc.getString(this.src)))) {
        ctx = this.jsonParseCtx.parse(stream);
      } catch (IOException e) {
        //do nothing this is a byte array stream close is a no-op. Throw stage exception just in case / futureproof.
        throw new StageException("Unexpected error parsing JSON.", e);
      }
    } else {
      ctx = this.jsonParseCtx.parse(doc.getString(this.src));
    }
    for (Entry<String, Object> entry : this.jsonFieldPaths.entrySet()) {
      JsonNode val = ctx.read((String) entry.getValue(), JsonNode.class);
      if (isValidNode(val)) { // makes sure that val and JsonNode Type is not null
        // note that if val is an empty String, will still be set in the document
        doc.setField(entry.getKey(), val);
      }
    }

    return null;
  }

  private boolean isValidNode(JsonNode val) {
    return val != null && !val.isNull();
  }

  private boolean isValidEntry(Entry<String, Object> entry) {
    return StringUtils.isNotBlank(entry.getKey()) && StringUtils.isNotBlank((String) entry.getValue());
  }
}
