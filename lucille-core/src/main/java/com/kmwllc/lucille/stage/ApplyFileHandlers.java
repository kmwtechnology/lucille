package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.connector.FileConnector;
import com.kmwllc.lucille.core.Spec;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.fileHandler.FileHandler;
import com.kmwllc.lucille.core.fileHandler.FileHandlerException;
import com.kmwllc.lucille.util.FileContentFetcher;
import com.typesafe.config.Config;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.io.FilenameUtils;

/**
 * Using a file path found on a Document, or a byte[] of its contents, applies file handlers to create children documents.
 * <p>
 * Config Parameters:
 * <ul>
 *   <li>
 *     fileHandlers (Map) : Specifies which file types should be handled / processed by this stage. Valid options include:
 *     <ul>
 *       <li>csv (Map, Optional) : For handling CSV files. Config will be passed to CSVFileHandler (if no alternate <code>class</code> is provided).</li>
 *       <li>json (Map, Optional) : For handling JSON or JSONL files. Config will be passed to JsonFileHandler (if no alternate <code>class</code> is provided).</li>
 *       <li>Custom <code>FileHandler</code> implementations.</li>
 *     </ul>
 *     <p> <b>Note:</b> An Exception is thrown if your <code>fileHandlers</code> Config is empty.
 *     <p> <b>Note:</b> See {@link FileHandler#createFromConfig(Config)} for notes on JSON and JSONL configuration.
 *     <p> <b>Note:</b> Lucille's XMLFileHandler is not supported.
 *   </li>
 *   <li>filePathField (String, Optional) : The field in your documents which will has the file path you want to apply handlers to.
 *   No processing will occur on documents without this field, even if they have the fileContentField present. Defaults to "file_path".</li>
 *   <li>fileContentField (String, Optional) : The field in your documents which has the file's contents as an array of bytes.
 *   When processing a document with a path to a supported file type, the handler will process the array of bytes found in this field,
 *   if present, before having the FileContentFetcher to open an InputStream for the file's contents. Defaults to "file_content".</li>
 *   <li>gcp (Map, Optional) : options for handling GoogleCloud files. Include if you are going to process documents with the filePathField set to a Google Cloud URI.
 *   See FileConnector for necessary arguments.</li>
 *   <li>s3 (Map, Optional) : options for handling S3 files. Include if you are going to process documents with the filePathField set to an S3 URI.
 *   See FileConnector for necessary arguments.</li>
 *   <li>azure (Map, Optional) : options for handling Azure files. Include if you are going to process documents with the filePathField set to an Azure URI.
 *   See FileConnector for necessary arguments.</li>
 * </ul>
 */
public class ApplyFileHandlers extends Stage {
  private final Config fileHandlersConfig;

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
        .withRequiredParentNames("fileHandlers")
        .withOptionalParents(
            FileConnector.GCP_PARENT_SPEC,
            FileConnector.AZURE_PARENT_SPEC,
            FileConnector.S3_PARENT_SPEC
        )
        .withOptionalProperties("filePathField", "fileContentField"));

    this.fileHandlersConfig = config.getConfig("fileHandlers");
    if (fileHandlersConfig.isEmpty()) {
      throw new IllegalArgumentException("Must specify at least one file handler.");
    }

    this.filePathField = ConfigUtils.getOrDefault(config, "filePathField", "file_path");
    this.fileContentField = ConfigUtils.getOrDefault(config, "fileContentField", "file_content");

    this.fileFetcher = new FileContentFetcher(config);
  }

  @Override
  public void start() throws StageException {
    this.fileHandlers = FileHandler.createFromConfig(fileHandlersConfig);

    if (fileHandlers.isEmpty()) {
      throw new StageException("No file handlers could be created from the given fileHandlers.");
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
