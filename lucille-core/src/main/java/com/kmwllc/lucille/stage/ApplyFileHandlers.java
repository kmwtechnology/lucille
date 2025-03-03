package com.kmwllc.lucille.stage;

import static com.kmwllc.lucille.core.fileHandler.FileHandler.SUPPORTED_FILE_TYPES;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.fileHandler.FileHandler;
import com.kmwllc.lucille.core.fileHandler.FileHandlerException;
import com.kmwllc.lucille.util.FileContentFetcher;
import com.typesafe.config.Config;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.io.FilenameUtils;

/**
 * Using a document's file path, applies file handlers to create children documents, as appropriate, using the file's content.
 *
 * handlerOptions (Map): Specifies which file types should be handled / processed by this stage. Valid options include:
 *  <br> csv (Map, Optional): csv config options for handling csv type files. Config will be passed to CSVFileHandler
 *  <br> json (Map, Optional): json config options for handling json/jsonl type files. Config will be passed to JsonFileHandler
 *
 *  <br> <b>Note:</b> handlerOptions should contain at least one of the above entries, otherwise, an Exception is thrown.
 *  <br> <b>Note:</b> XML is not supported.
 *
 * filePathField (String, Optional): Specify the field in your documents which will has the file path you want to apply handlers to.
 * Defaults to "file_path".
 *
 * gcp (Map, Optional): options for handling GoogleCloud files. Include if you are going to process documents with the filePathField set to a Google Cloud URI.
 * See FileConnector for necessary arguments.
 *
 * s3 (Map, Optional): options for handling S3 files. Include if you are going to process documents with the filePathField set to an S3 URI.
 * See FileConnector for necessary arguments.
 *
 * azure (Map, Optional): options for handling Azure files. Include if you are going to process documents with the filePathField set to an Azure URI.
 * See FileConnector for necessary arguments.
 *
 */
public class ApplyFileHandlers extends Stage {

  private final Config handlerOptions;

  private final String filePathField;
  private final FileContentFetcher fileFetcher;
  private final Map<String, FileHandler> fileHandlers;

  public ApplyFileHandlers(Config config) {
    super(config, new StageSpec()
        .withOptionalParents("handlerOptions", "gcp", "azure", "s3")
        .withOptionalProperties("filePathField"));

    this.handlerOptions = config.getConfig("handlerOptions");

    if (handlerOptions.isEmpty()) {
      throw new IllegalArgumentException("Must specify at least one file handler.");
    }

    this.filePathField = ConfigUtils.getOrDefault(config, "filePathField", "file_path");

    this.fileFetcher = new FileContentFetcher(config);
    this.fileHandlers = new HashMap<>();
  }

  @Override
  public void start() throws StageException {
    for (String fileExtensionSupported : SUPPORTED_FILE_TYPES) {
      if (handlerOptions.hasPath(fileExtensionSupported)) {
        FileHandler handler = FileHandler.create(fileExtensionSupported, handlerOptions);
        fileHandlers.put(fileExtensionSupported, handler);
      }
    }

    if (fileHandlers.isEmpty()) {
      throw new StageException("No file handlers could be created from the given handlerOptions.");
    }

    if (fileHandlers.containsKey("json")) {
      fileHandlers.put("jsonl", fileHandlers.get("json"));
    }

    try {
      fileFetcher.startup();
    } catch (IOException e) {
      throw new StageException("Error starting FileContentFetcher.", e);
    }
  }

  @Override
  public void stop() {
    fileFetcher.shutdown();
    fileHandlers.clear();
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (!doc.has(filePathField)) {
      return null;
    }

    String filePath = doc.getString(filePathField);
    String fileExtension = FilenameUtils.getExtension(filePath);

    if (!fileHandlers.containsKey(fileExtension)) {
      return null;
    }

    try {
      InputStream fileContentStream = fileFetcher.getInputStream(filePath);
      FileHandler handler = fileHandlers.get(fileExtension);

      return handler.processFile(fileContentStream, filePath);
    } catch (IOException e) {
      throw new StageException("Could not get InputStream for file " + filePath, e);
    } catch (FileHandlerException e) {
      throw new StageException("Could not process file " + filePath, e);
    }
  }
}
