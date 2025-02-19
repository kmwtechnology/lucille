package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.fileHandler.JsonFileHandler;
import com.kmwllc.lucille.util.FileUtils;
import com.typesafe.config.Config;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JSONConnector extends AbstractConnector {
  private static final Logger log = LoggerFactory.getLogger(JSONConnector.class);
  private final String pathStr;
  private final JsonFileHandler jsonFileHandler;

  public JSONConnector(Config config) {
    super(config);
    this.pathStr = config.getString("jsonPath");
    this.jsonFileHandler = new JsonFileHandler(config);
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    File file = new File(pathStr);
    Path path;
    try {
      path = file.toPath();
    } catch (Exception e) {
      throw new ConnectorException("Error converting " + pathStr + "to Path", e);
    }

    try {
      InputStream stream = FileUtils.getLocalInputStream(pathStr);
      log.debug("Processing file: {}", path);
      jsonFileHandler.processFileAndPublish(publisher, stream, path.toString());
    } catch (Exception e) {
      throw new ConnectorException("Error processing or publishing file: " + path, e);
    }
  }
}
