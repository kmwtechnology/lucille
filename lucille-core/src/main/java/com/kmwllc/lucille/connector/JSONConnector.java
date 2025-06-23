package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.Spec;
import com.kmwllc.lucille.core.fileHandler.CSVFileHandler;
import com.kmwllc.lucille.core.fileHandler.JsonFileHandler;
import com.kmwllc.lucille.util.FileContentFetcher;
import com.typesafe.config.Config;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connector implementation that produces documents from the rows in a given JSON or JSONL file.
 *
 * <br> The JSONConnector has been deprecated. Use {@link FileConnector} with a {@link JsonFileHandler}.
 */
@Deprecated
public class JSONConnector extends AbstractConnector {
  private static final Logger log = LoggerFactory.getLogger(JSONConnector.class);
  private final String pathStr;
  private final JsonFileHandler jsonFileHandler;

  public JSONConnector(Config config) {
    super(config, Spec.connector().withRequiredProperties("jsonPath"));
    this.pathStr = config.getString("jsonPath");
    this.jsonFileHandler = new JsonFileHandler(config
        .withoutPath("jsonPath")
        .withoutPath("name")
        .withoutPath("pipeline")
        .withoutPath("collapse"));
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    try {
      InputStream stream = FileContentFetcher.getOneTimeInputStream(pathStr);
      log.debug("Processing file: {}", pathStr);
      jsonFileHandler.processFileAndPublish(publisher, stream, pathStr);
    } catch (Exception e) {
      throw new ConnectorException("Error processing or publishing file: " + pathStr, e);
    }
  }
}
