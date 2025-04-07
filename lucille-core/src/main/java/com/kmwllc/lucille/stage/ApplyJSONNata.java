package com.kmwllc.lucille.stage;

import com.dashjoin.jsonata.JException;
import com.dashjoin.jsonata.Jsonata;
import com.kmwllc.lucille.core.Spec;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.DocumentException;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import static com.dashjoin.jsonata.Jsonata.jsonata;

/**
 * Applies a JSONNata expression to a JSON field or entire document treated as a JSON object. Since JSONNata is Turing complete this can 
 * theoretically carry out any transformation.
 *
 * TODO: Adjust these two lines...
 *  NOTE: Applying a transformation to an entire document is an experimental feature and should be used with caution.
 *  See <a href="https://github.com/IBM/JSONata4Java">Here</a> for JSONNata implementation.
 *
 * <br>
 * Config Parameters -
 * <br>
 * <p>
 * <b>source</b> (String, Optional) : The source field used as input. If not provided the entire document is used as input and the resulting json 
 * replaces the document. Logs warning and skips document if this transformation fails, mutates reserved fields, or returns a non-object (primitive or array).
 * </p>
 * <p>
 * <b>destination</b> (String, Optional) : The destination field into which the resulting json should be placed. If not provided, 
 * the source field is mutated in place.
 * </p>
 * <p>
 * <b>expression</b> (String) : The JSONNata expression
 * </p>
 */
public class ApplyJSONNata extends Stage {

  private static final Logger log = LoggerFactory.getLogger(ApplyJSONNata.class);

  private final String source;
  private final String destination;
  private final String expression;
  private Jsonata parsedExpression;

  public ApplyJSONNata(Config config) throws StageException {
    super(config, Spec.stage().withOptionalProperties("source", "destination").withRequiredProperties("expression"));
    this.source = ConfigUtils.getOrDefault(config, "source", null);
    this.destination = ConfigUtils.getOrDefault(config, "destination", null);
    this.expression = config.getString("expression");
  }

  @Override
  public void start() throws StageException {
    try {
      parsedExpression = jsonata(expression);
    } catch (JException e) {
      throw new StageException("Exception occurred while parsing expression: " + e.getLocalizedMessage());
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    // 1. Transform, if source == null
    if (source == null) {
      try {
        doc.transform(parsedExpression);
      } catch (DocumentException e) {
        log.warn("Exception occurred when applying transformation", e);
      }
      return null;
    }

    // 2. If !doc.has(source), return null
    if (!doc.has(source))  {
      return null;
    }

    // 3. JsonNode output, get parsedExpression.evaluate() doc's JSON
    Object output = parsedExpression.evaluate(doc.getJson(source).toString());

    // 4. put the output in destination / source areas
    if (destination != null) {
      doc.setField(destination, output);
    } else {
      doc.setField(source, output);
    }

    return null;
  }
}
