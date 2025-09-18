package com.kmwllc.lucille.connector;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.fileHandler.CSVFileHandler;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.util.FileContentFetcher;
import com.typesafe.config.Config;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.file.Path;

/**
 * Deprecated connector that produces documents from a CSV file; use FileConnector with CSVFileHandler.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>path (String, Required) : Path to the CSV file to read.</li>
 *   <li>moveToAfterProcessing (String, Optional) : Destination directory to move the file after successful processing.</li>
 *   <li>moveToErrorFolder (String, Optional) : Destination directory to move the file if processing fails.</li>
 *   <li>lineNumberField (String, Optional) : Document field to store the CSV row number.</li>
 *   <li>filenameField (String, Optional) : Document field to store the file name.</li>
 *   <li>filePathField (String, Optional) : Document field to store the full file path.</li>
 *   <li>idField (String, Optional) : Document field to use as the document ID.</li>
 *   <li>docIdFormat (String, Optional) : Format string for constructing document IDs when idField is used.</li>
 *   <li>separatorChar (String, Optional) : Column separator character to use.</li>
 *   <li>useTabs (Boolean, Optional) : Treat tab as the delimiter.</li>
 *   <li>interpretQuotes (Boolean, Optional) : Treat quotes as field wrappers.</li>
 *   <li>ignoreEscapeChar (Boolean, Optional) : Ignore the escape character during parsing.</li>
 *   <li>lowercaseFields (Boolean, Optional) : Lowercase header names before mapping to fields.</li>
 *   <li>ignoredTerms (List&lt;String&gt;, Optional) : Terms that cause rows to be skipped when matched.</li>
 * </ul>
 */

@Deprecated
public class CSVConnector extends AbstractConnector {

  private static final Logger log = LoggerFactory.getLogger(CSVConnector.class);
  private final CSVFileHandler csvFileHandler;
  private final String pathStr;
  private final String moveToAfterProcessing;
  private final String moveToErrorFolder;

  public static final Spec SPEC = SpecBuilder.connector()
      .requiredString("path")
      .optionalString("moveToAfterProcessing", "moveToErrorFolder", "lineNumberField", "filenameField", "filePathField",
          "idField", "docIdFormat", "separatorChar")
      .optionalBoolean("useTabs", "interpretQuotes", "ignoreEscapeChar", "lowercaseFields")
      .optionalList("ignoredTerms", new TypeReference<List<String>>(){}).build();

  public CSVConnector(Config config) {
    super(config);

    this.pathStr = config.getString("path");
    this.csvFileHandler = new CSVFileHandler(config
        .withoutPath("path")
        .withoutPath("name")
        .withoutPath("pipeline")
        .withoutPath("collapse")
        .withoutPath("moveToAfterProcessing")
        .withoutPath("moveToErrorFolder"));
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
