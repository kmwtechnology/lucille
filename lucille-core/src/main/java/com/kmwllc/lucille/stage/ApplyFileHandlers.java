package com.kmwllc.lucille.stage;

import static com.kmwllc.lucille.core.fileHandler.FileHandler.SUPPORTED_FILE_TYPES;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.fileHandler.FileHandler;
import com.kmwllc.lucille.core.fileHandler.FileHandlerException;
import com.typesafe.config.Config;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.io.FilenameUtils;

/**
 * Using a document's file path, applies file handlers to create children documents, as appropriate, using the file's content.
 *
 * HandlerOptions (Map): Specifies which file types should be handled / processed by this stage. Valid options include:
 *  <br> csv (Map, Optional): csv config options for handling csv type files. Config will be passed to CSVFileHandler
 *  <br> json (Map, Optional): json config options for handling json/jsonl type files. Config will be passed to JsonFileHandler
 *  TODO: Probably have to add "cloudOptions" to this stage. Pass it onto the FileHandlers.
 *
 * <br> <b>Note:</b> handler options should contain at least one of the above entries, otherwise, an Exception is thrown.
 * <br> <b>Note:</b> XML is not supported.
 */
public class ApplyFileHandlers extends Stage {
  private final Config handlerOptions;
  private final String filePathField;
  private final Map<String, FileHandler> fileHandlers;

  public ApplyFileHandlers(Config config) {
    super(config, new StageSpec().withOptionalParents("handlerOptions"));
    this.handlerOptions = config.getConfig("handlerOptions");

    if (handlerOptions.isEmpty()) {
      throw new IllegalArgumentException("Must specify at least one file handler.");
    }

    this.filePathField = ConfigUtils.getOrDefault(config, "filePathField", "source");
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
  }

  @Override
  public void stop() {
    fileHandlers.clear();
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (!doc.has(filePathField)) {
      return null;
    }

    String filePath = doc.getString(filePathField);
    String fileExtension = FilenameUtils.getExtension(filePath);

    if (FileHandler.supportAndContainFileType(fileExtension, handlerOptions)) {
      FileHandler handler = FileHandler.create(fileExtension, handlerOptions);

      Path path = Paths.get(filePath);

      try {
        return handler.processFile(path);
      } catch (FileHandlerException e) {
        throw new StageException("Could not process file: " + filePath + ".", e);
      }
    } else {
      return null;
    }
  }
}
