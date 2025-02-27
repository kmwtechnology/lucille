package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.fileHandler.CSVFileHandler;
import com.kmwllc.lucille.util.FileContentFetcher;
import com.typesafe.config.Config;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.file.Path;

/**
 * Connector implementation that produces documents from the rows in a given CSV file.
 */
@Deprecated
public class CSVConnector extends AbstractConnector {

  private static final Logger log = LoggerFactory.getLogger(CSVConnector.class);
  private final CSVFileHandler csvFileHandler;
  private final String pathStr;
  private final String moveToAfterProcessing;
  private final String moveToErrorFolder;

  public CSVConnector(Config config) {
    super(config);
    this.pathStr = config.getString("path");
    this.csvFileHandler = new CSVFileHandler(config);
    this.moveToAfterProcessing = config.hasPath("moveToAfterProcessing") ? config.getString("moveToAfterProcessing") : null;
    this.moveToErrorFolder = config.hasPath("moveToErrorFolder") ? config.getString("moveToErrorFolder") : null;
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    File file = new File(pathStr);
    Path path;
    try {
      path = file.toPath();
    } catch (InvalidPathException e) {
      throw new ConnectorException("Error converting " + pathStr + "to Path", e);
    }

    createProcessedAndErrorFoldersIfSet();

    try {
      InputStream stream = FileContentFetcher.getOneTimeInputStream(pathStr);
      log.debug("Processing file: {}", pathStr);
      csvFileHandler.processFileAndPublish(publisher, stream, pathStr);
    } catch (Exception e) {
      if (moveToErrorFolder != null) {
        // move to error folder
        moveFile(path.toAbsolutePath().normalize(), moveToErrorFolder);
      }
      throw new ConnectorException("Error processing or publishing file: " + path, e);
    }

    if (moveToAfterProcessing != null) {
      // move to processed folder
      moveFile(path.toAbsolutePath().normalize(), moveToAfterProcessing);
    }
  }

  public String toString() {
    return "CSVConnector: " + pathStr;
  }

  public void createProcessedAndErrorFoldersIfSet() {
    if (moveToAfterProcessing != null) {
      // Create the destination directory if it doesn't exist.
      File destDir = new File(moveToAfterProcessing);
      if (!destDir.exists()) {
        log.info("Creating archive directory {}", destDir.getAbsolutePath());
        destDir.mkdirs();
      }
    }

    if (moveToErrorFolder != null) {
      File errorDir = new File(moveToErrorFolder);
      if (!errorDir.exists()) {
        log.info("Creating error directory {}", errorDir.getAbsolutePath());
        errorDir.mkdirs();
      }
    }
  }

  public void moveFile(Path absolutePath, String option) {
    if (absolutePath.startsWith("classpath:")) {
      log.warn("Skipping moving classpath file: {} to {}", absolutePath, moveToAfterProcessing);
      return;
    }

    String fileName = absolutePath.getFileName().toString();
    Path dest = Paths.get(option + File.separatorChar + fileName);
    try {
      Files.move(absolutePath, dest);
      log.debug("File {} was successfully moved from source {} to destination {}", fileName, absolutePath, dest);
    } catch (IOException e) {
      log.warn("Error moving file to destination directory", e);
    }
  }
}
