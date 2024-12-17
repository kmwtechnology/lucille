package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.fileHandler.CSVFileHandler;
import com.typesafe.config.Config;
import java.io.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.file.Path;

/**
 * Connector implementation that produces documents from the rows in a given CSV file.
 */
public class CSVConnector extends AbstractConnector {

  private static final Logger log = LoggerFactory.getLogger(CSVConnector.class);
  private final CSVFileHandler csvFileHandler;
  private final String pathStr;

  public CSVConnector(Config config) {
    super(config);
    this.pathStr = config.getString("path");
    this.csvFileHandler = new CSVFileHandler(config);
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    File file = new File(pathStr);
    Path path = file.toPath();
    try {
      // validate that the path is a valid file and create necessary directories
      csvFileHandler.beforeProcessingFile(path);
    } catch (Exception e) {
      throw new ConnectorException("Error before processing file: " + path, e);
    }

    try {
      log.info("Processing file: {}", path);
      csvFileHandler.processFileAndPublish(publisher, path);
    } catch (Exception e) {
      csvFileHandler.errorProcessingFile(path);
      throw new ConnectorException("Error processing or publishing file: " + path, e);
    }

    try {
      csvFileHandler.afterProcessingFile(path);
    } catch (Exception e) {
      throw new ConnectorException("Error after processing file: " + path, e);
    }
  }

  public String toString() {
    return "CSVConnector: " + pathStr;
  }
}
