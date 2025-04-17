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
 * Applies a given Jsonata expression to extract information from a Document's field or to transform a Document entirely.
 * Applying a transformation to an entire document is an experimental feature and should be used with caution.
 *
 * See <a href="https://github.com/dashjoin/jsonata-java">here</a> for Jsonata implementation.
 *
 * <br>
 * Config Parameters -
 * <p> <b>source</b> (String, Optional) : The field to use for input. Defaults to applying your expression the entire Document and mutating the entire
 * Document in place. Logs warning and skips document if this transformation fails, mutates reserved fields, or returns a non-object (primitive or array).
 * <p> <b>destination</b> (String, Optional) : The destination field into which the json response should be placed. Defaults to mutating the source
 * field. Has no effect if source is not specified.
 * <p> <b>expression</b> (String) : The Jsonata expression you want to apply to each Document.
 */
public class ApplyJSONNata extends Stage {

  private static final Logger log = LoggerFactory.getLogger(ApplyJSONNata.class);

  private final String source;
  private final String destination;
  private final String expressionStr;
  private Jsonata parsedExpression;

  public ApplyJSONNata(Config config) throws StageException {
    super(config, Spec.stage().withOptionalProperties("source", "destination").withRequiredProperties("expression"));
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
    if (source == null) {
      try {
        doc.transform(parsedExpression);
      } catch (DocumentException e) {
        log.warn("Exception occurred when applying transformation", e);
      }
      return null;
    }

    if (!doc.has(source))  {
      return null;
    }

    Object output = parsedExpression.evaluate(doc.getJson(source).toString());

    if (destination != null) {
      doc.setField(destination, output);
    } else {
      doc.setField(source, output);
    }

    return null;
  }
}
