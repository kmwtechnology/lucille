package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.fileHandler.JsonFileHandler;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.util.FileContentFetcher;
import com.typesafe.config.Config;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deprecated connector that produces documents from a JSON or JSONL file; use FileConnector with JsonFileHandler.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>jsonPath (String, Required) : Path to the JSON or JSONL file to read.</li>
 * </ul>
 */
@Deprecated
public class JSONConnector extends AbstractConnector {
  private static final Logger log = LoggerFactory.getLogger(JSONConnector.class);
  private final String pathStr;
  private final JsonFileHandler jsonFileHandler;

  public static final Spec SPEC = SpecBuilder.connector().requiredString("jsonPath").build();

  public JSONConnector(Config config) {
    super(config);
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
