package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.connector.FileConnector;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.fileHandler.CSVFileHandler;
import com.kmwllc.lucille.core.fileHandler.FileHandler;
import com.kmwllc.lucille.core.fileHandler.FileHandlerException;
import com.kmwllc.lucille.core.fileHandler.JsonFileHandler;
import com.kmwllc.lucille.util.FileContentFetcher;
import com.typesafe.config.Config;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.io.FilenameUtils;

/**
 * <p> Using a file path found on a Document, applies file handlers to create children documents, as appropriate, using the file's content.
 *
 * <p> filePathField (String, Optional): Specify the field in your documents which will has the file path you want to apply handlers to.
 * No processing will occur on documents without this field, even if they have the fileContentField present. Defaults to "file_path".
 *
 * <p> fileContentField (String, Optional): Specify the field in your documents which has the file's contents as an array of bytes.
 * When processing a document with a path to a supported file type, the handler will process the array of bytes found in this field,
 * if present, before having the FileContentFetcher to open an InputStream for the file's contents. Defaults to "file_content".
 *
 * <p> handlerOptions (Map): Specifies which file types should be handled / processed by this stage. Valid options include:
 * <p>   csv (Map, Optional): csv config options for handling csv type files. Config will be passed to CSVFileHandler
 * <p>   json (Map, Optional): json config options for handling json/jsonl type files. Config will be passed to JsonFileHandler
 *
 * <p> <b>Note:</b> handlerOptions should contain at least one of the above entries, otherwise, an Exception is thrown.
 * <p> <b>Note:</b> XML is not supported.
 *
 * <p> gcp (Map, Optional): options for handling GoogleCloud files. Include if you are going to process documents with the filePathField set to a Google Cloud URI.
 * See FileConnector for necessary arguments.
 *
 * <p> s3 (Map, Optional): options for handling S3 files. Include if you are going to process documents with the filePathField set to an S3 URI.
 * See FileConnector for necessary arguments.
 *
 * <p> azure (Map, Optional): options for handling Azure files. Include if you are going to process documents with the filePathField set to an Azure URI.
 * See FileConnector for necessary arguments.
 */
public class ApplyFileHandlers extends Stage {
  private final Config handlerOptions;

  private final String filePathField;
  private final String fileContentField;
  private final FileContentFetcher fileFetcher;

  private Map<String, FileHandler> fileHandlers;

  /**
   * Creates the ApplyFileHandlers stage from the given config.
   * @param config Configuration for the ApplyFileHandlers stage.
   */
  public ApplyFileHandlers(Config config) {
    super(config, Spec.stage()
        .optParent(
            Spec.parent("handlerOptions")
                .optParent(CSVFileHandler.PARENT_SPEC, JsonFileHandler.PARENT_SPEC),
            FileConnector.GCP_PARENT_SPEC,
            FileConnector.AZURE_PARENT_SPEC,
            FileConnector.S3_PARENT_SPEC)
        .withOptionalProperties("filePathField", "fileContentField"));

    this.handlerOptions = config.getConfig("handlerOptions");
    if (handlerOptions.isEmpty()) {
      throw new IllegalArgumentException("Must specify at least one file handler.");
    }

    this.filePathField = ConfigUtils.getOrDefault(config, "filePathField", "file_path");
    this.fileContentField = ConfigUtils.getOrDefault(config, "fileContentField", "file_content");

    this.fileFetcher = new FileContentFetcher(config);
  }

  @Override
  public void start() throws StageException {
    this.fileHandlers = FileHandler.createFromConfig(handlerOptions);
    if (fileHandlers.isEmpty()) {
      throw new StageException("No file handlers could be created from the given handlerOptions.");
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

    FileHandler handler = fileHandlers.get(fileExtension);
    InputStream fileContentStream;

    if (doc.has(fileContentField)) {
      byte[] fileContents = doc.getBytes(fileContentField);
      fileContentStream = new ByteArrayInputStream(fileContents);
    } else {
      try {
        fileContentStream = fileFetcher.getInputStream(filePath);
      } catch (IOException e) {
        throw new StageException("Could not fetch InputStream for file " + filePath, e);
      }
    }

    try {
      return handler.processFile(fileContentStream, filePath);
    } catch (FileHandlerException e) {
      throw new StageException("Could not process file " + filePath, e);
    }
  }
}
