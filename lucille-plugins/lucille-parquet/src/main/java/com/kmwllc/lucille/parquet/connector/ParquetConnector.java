package com.kmwllc.lucille.parquet.connector;

import com.kmwllc.lucille.connector.AbstractConnector;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetConnector extends AbstractConnector {
  private final String pathStr;

  private final ParquetFileHandler parquetFileHandler;

  private static final Logger log = LoggerFactory.getLogger(ParquetConnector.class);

  public ParquetConnector(Config config) {
    super(config);
    this.pathStr = config.getString("path");

    this.parquetFileHandler = new ParquetFileHandler(config);
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    java.nio.file.Path javaPath;

    try {
      javaPath = Paths.get(pathStr);
    } catch (Exception e) {
      throw new ConnectorException("Error creating path from String " + pathStr, e);
    }

    try {
      log.debug("Processing file: {}", javaPath);
      parquetFileHandler.processFileAndPublish(publisher, javaPath);
    } catch (Exception e) {
      throw new ConnectorException("Error processing or publishing file: " + javaPath, e);
    }
  }
}
