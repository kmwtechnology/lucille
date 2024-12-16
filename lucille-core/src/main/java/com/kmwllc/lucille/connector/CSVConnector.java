package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.fileHandlers.CSVFileTypeHandler;
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
  private final CSVFileTypeHandler csvFileHandler;
  private final String pathStr;

  public CSVConnector(Config config) {
    super(config);
    this.pathStr = config.getString("path");
    this.csvFileHandler = new CSVFileTypeHandler(config);
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    File pathFile = new File(pathStr);
    Path filePath = pathFile.toPath();
    try {
      // validate that the path is a valid file and create necessary directories
      csvFileHandler.beforeProcessingFile(filePath);
    } catch (Exception e) {
      throw new ConnectorException("Error before processing file: " + filePath, e);
    }

    try {
      log.info("Processing file: {}", filePath);
      csvFileHandler.processFileAndPublish(publisher, filePath);
    } catch (Exception e) {
      csvFileHandler.errorProcessingFile(filePath);
      throw new ConnectorException("Error processing or publishing file: " + filePath, e);
    }

    try {
      csvFileHandler.afterProcessingFile(filePath);
    } catch (Exception e) {
      throw new ConnectorException("Error after processing file: " + filePath, e);
    }
  }

  public String toString() {
    return "CSVConnector: " + pathStr;
  }
}
