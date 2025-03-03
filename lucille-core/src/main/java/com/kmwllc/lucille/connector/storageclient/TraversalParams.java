package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.FileConnector.GET_FILE_CONTENT;
import static com.kmwllc.lucille.connector.FileConnector.HANDLE_ARCHIVED_FILES;
import static com.kmwllc.lucille.connector.FileConnector.HANDLE_COMPRESSED_FILES;
import static com.kmwllc.lucille.connector.FileConnector.MOVE_TO_AFTER_PROCESSING;
import static com.kmwllc.lucille.connector.FileConnector.MOVE_TO_ERROR_FOLDER;

import com.kmwllc.lucille.core.fileHandler.FileHandler;
import com.typesafe.config.Config;
import java.net.URI;
import java.util.List;
import java.util.regex.Pattern;

/**
 * The arguments / parameters associated with a traversal on a StorageClient.
 */
public class TraversalParams {

  // provided arguments
  private final URI pathToStorageURI;
  private final String docIdPrefix;
  private final List<Pattern> excludes;
  private final List<Pattern> includes;
  private final Config fileOptions;

  // derived arguments
  private final boolean getFileContent;
  private final boolean handleArchivedFiles;
  private final boolean handleCompressedFiles;
  private final String moveToAfterProcessing;
  private final String moveToErrorFolder;

  public TraversalParams(URI pathToStorageURI, String docIdPrefix, List<Pattern> includes, List<Pattern> excludes, Config fileOptions) {
    this.pathToStorageURI = pathToStorageURI;
    this.docIdPrefix = docIdPrefix;
    this.includes = includes;
    this.excludes = excludes;
    this.fileOptions = fileOptions;
    this.getFileContent = !fileOptions.hasPath(GET_FILE_CONTENT) || fileOptions.getBoolean(GET_FILE_CONTENT);
    this.handleArchivedFiles = fileOptions.hasPath(HANDLE_ARCHIVED_FILES) && fileOptions.getBoolean(HANDLE_ARCHIVED_FILES);
    this.handleCompressedFiles = fileOptions.hasPath(HANDLE_COMPRESSED_FILES) && fileOptions.getBoolean(HANDLE_COMPRESSED_FILES);
    this.moveToAfterProcessing = fileOptions.hasPath(MOVE_TO_AFTER_PROCESSING) ? fileOptions.getString(MOVE_TO_AFTER_PROCESSING) : null;
    this.moveToErrorFolder = fileOptions.hasPath(MOVE_TO_ERROR_FOLDER) ? fileOptions.getString(MOVE_TO_ERROR_FOLDER) : null;
  }

  /**
   * Returns whether the given file associated with the given path String should be processed, based on this object's
   * includes/excludes patterns.
   */
  public boolean shouldIncludeFile(String pathStr) {
    return excludes.stream().noneMatch(pattern -> pattern.matcher(pathStr).matches())
        && (includes.isEmpty() || includes.stream().anyMatch(pattern -> pattern.matcher(pathStr).matches()));
  }

  /**
   * Returns whether the fileOptions associated with these params include the given file extension, meaning an attempt
   * should be made to build a FileHandler for the given extension.
   */
  public boolean optionsIncludeFileExtension(String fileExtension) {
    return getFileOptions().hasPath(fileExtension);
  }

  /**
   * Returns whether a file with the given extension is supported, as per these TraversalParams. Handles the nuance
   * of json supporting jsonl and vice versa.
   */
  public boolean supportedFileType(String fileExtension) {
    return !getFileOptions().isEmpty() && FileHandler.supportAndContainFileType(fileExtension, getFileOptions());
  }

  public URI getPathToStorageURI() {
    return pathToStorageURI;
  }

  public String getDocIdPrefix() {
    return docIdPrefix;
  }

  public Config getFileOptions() {
    return fileOptions;
  }

  public boolean shouldGetFileContent() {
    return getFileContent;
  }

  public boolean getHandleArchivedFiles() {
    return handleArchivedFiles;
  }

  public boolean getHandleCompressedFiles() {
    return handleCompressedFiles;
  }

  public String getMoveToAfterProcessing() {
    return moveToAfterProcessing;
  }

  public String getMoveToErrorFolder() {
    return moveToErrorFolder;
  }
}
