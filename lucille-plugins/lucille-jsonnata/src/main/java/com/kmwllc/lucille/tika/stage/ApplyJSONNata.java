package com.kmwllc.lucille.tika.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.FileUtils;
import com.typesafe.config.Config;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

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
    if (!doc.has(this.source)) {
      return null;
    }
    
    Expressions.parse("string");
    



    return null;
  }

}
