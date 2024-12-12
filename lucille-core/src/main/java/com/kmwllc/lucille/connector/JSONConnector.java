package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.fileHandlers.JsonFileHandler;
import com.typesafe.config.Config;
import java.io.File;
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
    File pathFile = new File(pathStr);
    Path filePath = pathFile.toPath();
    try {
      jsonFileHandler.beforeProcessingFile(filePath);
    } catch (Exception e) {
      throw new ConnectorException("Error before processing file: " + filePath, e);
    }

    try {
      log.info("Processing file: {}", filePath);
      jsonFileHandler.processFileAndPublish(publisher, filePath);
    } catch (Exception e) {
      jsonFileHandler.errorProcessingFile(filePath);
      throw new ConnectorException("Error processing file: " + filePath, e);
    }

    try {
      jsonFileHandler.afterProcessingFile(filePath);
    } catch (Exception e) {
      throw new ConnectorException("Error after processing file: " + filePath, e);
    }
  }
}
