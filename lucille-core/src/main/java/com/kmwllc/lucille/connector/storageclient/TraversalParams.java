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
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * The arguments / parameters associated with a traversal on a StorageClient.
 */
public class TraversalParams {

  // provided arguments
  public final URI pathToStorageURI;
  public final String docIdPrefix;
  private final List<Pattern> excludes;
  private final List<Pattern> includes;
  public final Config fileOptions;

  // derived arguments
  public final boolean getFileContent;
  public final String bucketOrContainerName;
  public final boolean handleArchivedFiles;
  public final boolean handleCompressedFiles;
  public final String moveToAfterProcessing;
  public final String moveToErrorFolder;
  public final String startingDirectory;

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

    // NOTE: StorageClients should use their getStartingDirectory and getBucketOrContainerName methods to
    // return these properties, as AzureStorageClient has different values it will return.
    this.bucketOrContainerName = pathToStorageURI.getAuthority();
    this.startingDirectory = getStartingDirectory();
  }

  private String getStartingDirectory() {
    String startingDirectory = Objects.equals(pathToStorageURI.getPath(), "/") ? "" : pathToStorageURI.getPath();
    if (startingDirectory.startsWith("/")) return startingDirectory.substring(1);
    return startingDirectory;
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
    return fileOptions.hasPath(fileExtension);
  }

  /**
   * Returns whether a file with the given extension is supported, as per these TraversalParams. Handles the nuance
   * of json supporting jsonl and vice versa.
   */
  public boolean supportedFileType(String fileExtension) {
    return !fileOptions.isEmpty() && FileHandler.supportAndContainFileType(fileExtension, fileOptions);
  }
}
