package com.kmwllc.lucille.stage;

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

public class ApplyJSONNata extends Stage {

  private static final Logger log = LoggerFactory.getLogger(ApplyJSONNata.class);

  private final String source;
  private final String destination;
  private final String expression;
  private Expressions parsed;

  public ApplyJSONNata(Config config) throws StageException {
    super(config, new StageSpec().withOptionalProperties("source", "destination").withRequiredProperties("expression"));
    this.source = ConfigUtils.getOrDefault(config, "source", null);
    this.destination = ConfigUtils.getOrDefault(config, "destination", null);
    this.expression = config.getString("expression");
  }

  @Override
  public void start() throws StageException {
    try {
      parsed = Expressions.parse(expression);
    } catch (ParseException e) {
      throw new StageException("Exception occured while parsing expression: " + e.getLocalizedMessage());
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
        log.warn("Exception occured when applying transformation", e);
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
      log.warn("Evaluation exception occured when applying transformation: ", e);
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
