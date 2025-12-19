package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.lang3.StringUtils;

/**
 * Parses a JSON string and sets fields on the processed document according to the configured mapping using
 * JsonPath expressions. If no JsonPath expressions are provided, all JSON fields are copied to the document.
 * @see <a href="https://github.com/json-path/JsonPath">JsonPath</a>
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>src (String) : The field containing the JSON string to be parsed.</li>
 *   <li>sourceIsBase64: When set to true, indicates that the source field is base64 encoded. In this case the stage will decode
 *   the field value before parsing.</li>
 *   <li>jsonFieldPaths (Map&lt;String, Object&gt;) : Defines the mapping from JsonPath expressions to the destination fields in the
 *   processed document. Omit this property if you want all fields from the JSON to be copied to the top level of the document.</li>
 *   <li>update_mode (String, Optional) : Determines how writing will be handling if the destination field is already populated. Can
 *   be 'overwrite', 'append' or 'skip'. Defaults to 'overwrite'.</li>
 * </ul>
 */
public class ParseJson extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .requiredString("src")
      .optionalBoolean("sourceIsBase64")
      .optionalString("update_mode")
      .optionalParent("jsonFieldPaths", new TypeReference<Map<String, Object>>() {}).build();

  private static final ObjectMapper mapper = new ObjectMapper();

  private final String src;
  private final Map<String, Object> jsonFieldPaths;
  private final Configuration jsonPathConf;
  private final boolean sourceIsBase64;
  private final UpdateMode updateMode;

  private ParseContext jsonParseCtx;

  public ParseJson(Config config) {
    super(config);

    this.src = config.getString("src");
    this.jsonFieldPaths = config.hasPath("jsonFieldPaths") ? config.getConfig("jsonFieldPaths").root().unwrapped() : null;
    this.sourceIsBase64 = config.hasPath("sourceIsBase64") && config.getBoolean("sourceIsBase64");
    this.updateMode = UpdateMode.fromConfig(config);

    if (this.jsonFieldPaths != null) {
      this.jsonPathConf = Configuration.builder()
          .jsonProvider(new JacksonJsonProvider())
          .mappingProvider(new JacksonMappingProvider())
          .options(Option.DEFAULT_PATH_LEAF_TO_NULL, Option.SUPPRESS_EXCEPTIONS).build();
    } else {
      this.jsonPathConf = null;
    }
  }

  @Override
  public void start() throws StageException {
    if (jsonFieldPaths != null) {
      jsonParseCtx = JsonPath.using(jsonPathConf);

      for (Entry<String, Object> entry : jsonFieldPaths.entrySet()) {
        if (StringUtils.isBlank(entry.getKey()) || StringUtils.isBlank((String) entry.getValue())) {
          throw new StageException("jsonFieldPaths mapping contains a blank or null key/value.");
        }
      }
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (!doc.has(src)) {
      return null;
    }

    // if src is a base-64-encoded string, doc.getBytes() will transparently decode it
    String jsonString = sourceIsBase64 ? new String(doc.getBytes(src)) : doc.getString(src);

    // if jsonFieldPaths is present, we parse the document using a ParseContext instead of an ObjectMapper,
    // and evaluate each provided json path expression to set the corresponding document field
    if (jsonFieldPaths != null) {
      DocumentContext ctx;
      try {
        ctx = jsonParseCtx.parse(jsonString);
      } catch (Exception e) {
        throw new StageException("Failed to parse JSON in document " + doc.getId() + " with src field: " + src, e);
      }

      for (Entry<String, Object> entry : jsonFieldPaths.entrySet()) {
        JsonNode val = ctx.read((String) entry.getValue(), JsonNode.class);
        if (val != null && !val.isNull()) {
          doc.update(entry.getKey(), updateMode, val);
        }
      }
      return null;
    }

    // if jsonFieldPaths is absent, we parse the document using an ObjectMapper instead of a ParseContext,
    // and we copy all JSON fields to the document
    JsonNode node;
    try {
      node = mapper.readTree(jsonString);
    } catch (Exception e) {
      throw new StageException("Failed to parse JSON in document " + doc.getId() + " with src field: " + src, e);
    }

    if (!node.isObject()) {
      throw new StageException("Non-object JSON in document " + doc.getId() + " with src field: " + src);
    }

    Iterator<Entry<String,JsonNode>> fields = node.fields();
    while (fields.hasNext()) {
      Entry<String,JsonNode> field = fields.next();
      doc.update(field.getKey(), updateMode, field.getValue());
    }

    return null;
  }

}
