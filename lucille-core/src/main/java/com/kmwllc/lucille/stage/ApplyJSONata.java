package com.kmwllc.lucille.stage;

import com.dashjoin.jsonata.JException;
import com.dashjoin.jsonata.Jsonata;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import static com.dashjoin.jsonata.Jsonata.jsonata;

/**
 * Applies a given Jsonata expression to extract information from a Document's field or to transform a Document entirely.
 * Applying a transformation to an entire document is an experimental feature and should be used with caution.
 * See <a href="https://github.com/dashjoin/jsonata-java">here</a> for Jsonata implementation.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>source (String, Optional) : The field to use for input. Defaults to applying your expression the entire Document and mutating the entire
 *   Document in place. Logs warning and skips document if this transformation fails, mutates reserved fields, or returns a non-object (primitive or array).</li>
 *   <li>destination (String, Optional) : The destination field into which the json response should be placed. Defaults to mutating the source
 *   field. Has no effect if source is not specified.</li>
 *   <li>expression (String) : The Jsonata expression you want to apply to each Document.</li>
 * </ul>
 */
public class ApplyJSONata extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .requiredString("expression")
      .optionalString("source", "destination").build();

  private static final Logger log = LoggerFactory.getLogger(ApplyJSONata.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private final String source;
  private final String destination;
  private final String expressionStr;
  private Jsonata parsedExpression;

  public ApplyJSONata(Config config) throws StageException {
    super(config);
    this.source = ConfigUtils.getOrDefault(config, "source", null);
    this.destination = ConfigUtils.getOrDefault(config, "destination", null);
    this.expressionStr = config.getString("expression");
  }

  @Override
  public void start() throws StageException {
    try {
      parsedExpression = jsonata(expressionStr);
    } catch (JException e) {
      throw new StageException("Exception occurred while parsing expression: " + e.getLocalizedMessage());
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    try {
      if (source == null) { // transform on whole doc
        doc.transform(parsedExpression);
      } else {
        if (!doc.has(source))  {
          log.info("Field {} not found on Document, Jsonata will not be applied.", source);
          return null;
        }
        doc.transform(parsedExpression, source, destination);
      }
    } catch(Exception e) {
      log.warn("Exception occurred when applying transformation to document ({}). No change will take place.", doc.getId(), e);
    }

    return null;
  }
}
