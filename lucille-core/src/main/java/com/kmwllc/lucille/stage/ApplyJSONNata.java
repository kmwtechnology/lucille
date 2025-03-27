package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.ConfigSpec;
import java.io.IOException;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.api.jsonata4java.expressions.EvaluateException;
import com.api.jsonata4java.expressions.Expressions;
import com.api.jsonata4java.expressions.ParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.DocumentException;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

/**
 * Applies a JSONNata expression to a JSON field or entire document treated as a JSON object. Since JSONNata is Turing complete this can 
 * theoretically carry out any transformation. NOTE: Applying a transformation to an entire document is an experimental feature and should be used with caution.
 * See <a href="https://github.com/IBM/JSONata4Java">Here</a> for JSONNata implementation.
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
  private Expressions parsed;

  public ApplyJSONNata(Config config) throws StageException {
    super(config, new ConfigSpec().withOptionalProperties("source", "destination").withRequiredProperties("expression"));
    this.source = ConfigUtils.getOrDefault(config, "source", null);
    this.destination = ConfigUtils.getOrDefault(config, "destination", null);
    this.expression = config.getString("expression");
  }

  @Override
  public void start() throws StageException {
    try {
      parsed = Expressions.parse(expression);
    } catch (ParseException e) {
      throw new StageException("Exception occurred while parsing expression: " + e.getLocalizedMessage());
    } catch (IOException e) {
      throw new StageException("IO exception while parsing expression: " + e.getLocalizedMessage());
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (source == null) {
      try {
        doc.transform(parsed);
      } catch (DocumentException e) {
        log.warn("Exception occurred when applying transformation", e);
      }
      return null;
    }

    if (!doc.has(source)) {
      return null;
    }

    JsonNode output = null;
    try {
      output = parsed.evaluate(doc.getJson(source));
    } catch (EvaluateException e) {
      log.warn("Evaluation exception occurred when applying transformation: ", e);
      return null;
    }

    if (destination != null) {
      doc.setField(destination, output);
    } else {
      doc.setField(source, output);
    }

    return null;
  }
}
