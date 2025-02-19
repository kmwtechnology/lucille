package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.fileHandler.FileHandler;
import com.kmwllc.lucille.core.fileHandler.FileHandlerException;
import com.typesafe.config.Config;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import org.apache.commons.io.FilenameUtils;

/**
 * Using a document's file path, applies file handlers to create children documents, as appropriate, using the file's content.
 *
 * HandlerOptions:
 *  csv (Map, Optional): csv config options for handling csv type files. Config will be passed to CSVFileHandler
 *  json (Map, Optional): json config options for handling json/jsonl type files. Config will be passed to JsonFileHandler
 *
 * <br> <b>Note:</b> handler options should contain at least one of the above entries, otherwise, an Exception is thrown.
 * <br> <b>Note:</b> XML is not supported.
 */
public class ApplyFileHandlers extends Stage {
  private final String filePathField;
  private final Config handlerOptions;

  public ApplyFileHandlers(Config config) {
    super(config, new StageSpec().withRequiredParents("handlerOptions"));
    this.filePathField = ConfigUtils.getOrDefault(config, "filePathField", "source");
    this.handlerOptions = config.getConfig("handlerOptions");
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (!doc.has(filePathField)) {
      return null;
    }

    // TODO: Need to make sure to support cloud and not just local...
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
