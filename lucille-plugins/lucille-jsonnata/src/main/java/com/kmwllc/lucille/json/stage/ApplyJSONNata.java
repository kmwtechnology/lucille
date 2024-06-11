package com.kmwllc.lucille.json.stage;

import com.fasterxml.jackson.databind.JsonNode;
import com.api.jsonata4java.expressions.Expressions;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import java.util.Iterator;

public class ApplyJSONNata extends Stage {

  private final String source;
  private final String destination;
  private final String expression;

  public ApplyJSONNata(Config config) throws StageException {
    super(config, new StageSpec().withOptionalProperties("source", "destination").withRequiredProperties("expression"));
    this.source = ConfigUtils.getOrDefault(config, "source", null);
    this.destination = ConfigUtils.getOrDefault(config, "destination", null);
    this.expression = config.getString("expression");
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (!doc.has(source)) {
      return null;
    }
    
    JsonNode output = Expressions.parse(expression).evaluate(doc.getJson(source));
    



    return null;
  }

}
