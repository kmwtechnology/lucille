package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.FileConnector.GET_FILE_CONTENT;
import static com.kmwllc.lucille.connector.FileConnector.HANDLE_ARCHIVED_FILES;
import static com.kmwllc.lucille.connector.FileConnector.HANDLE_COMPRESSED_FILES;
import static com.kmwllc.lucille.connector.FileConnector.MOVE_TO_AFTER_PROCESSING;
import static com.kmwllc.lucille.connector.FileConnector.MOVE_TO_ERROR_FOLDER;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.fileHandler.FileHandler;
import com.typesafe.config.Config;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * The arguments / parameters associated with a traversal on a StorageClient.
 */
public class TraversalParams {

  // provided arguments
  private final URI uri;
  private final String docIdPrefix;

  // FileOptions / its params
  private final Config fileOptions;
  private final boolean getFileContent;
  private final boolean handleArchivedFiles;
  private final boolean handleCompressedFiles;
  private final String moveToAfterProcessing;
  private final String moveToErrorFolder;

  // FilterOptions / its params
  private final List<Pattern> excludes;
  private final List<Pattern> includes;
  private final Duration modificationCutoff;

  public TraversalParams(URI uri, String docIdPrefix, Config fileOptions, Config filterOptions) {
    this.uri = uri;
    this.docIdPrefix = docIdPrefix;

    // file options / derived params
    this.fileOptions = fileOptions;
    this.getFileContent = !fileOptions.hasPath(GET_FILE_CONTENT) || fileOptions.getBoolean(GET_FILE_CONTENT);
    this.handleArchivedFiles = fileOptions.hasPath(HANDLE_ARCHIVED_FILES) && fileOptions.getBoolean(HANDLE_ARCHIVED_FILES);
    this.handleCompressedFiles = fileOptions.hasPath(HANDLE_COMPRESSED_FILES) && fileOptions.getBoolean(HANDLE_COMPRESSED_FILES);
    this.moveToAfterProcessing = fileOptions.hasPath(MOVE_TO_AFTER_PROCESSING) ? fileOptions.getString(MOVE_TO_AFTER_PROCESSING) : null;
    this.moveToErrorFolder = fileOptions.hasPath(MOVE_TO_ERROR_FOLDER) ? fileOptions.getString(MOVE_TO_ERROR_FOLDER) : null;

    // filter options / derived params
    List<String> includeRegex = filterOptions.hasPath("includes") ?
        filterOptions.getStringList("includes") : Collections.emptyList();
    this.includes = includeRegex.stream().map(Pattern::compile).collect(Collectors.toList());
    List<String> excludeRegex = filterOptions.hasPath("excludes") ?
        filterOptions.getStringList("excludes") : Collections.emptyList();
    this.excludes = excludeRegex.stream().map(Pattern::compile).collect(Collectors.toList());

    // TODO: make sure to handle the null value appropriately.
    this.modificationCutoff = ConfigUtils.getOrDefault(filterOptions, "modificationCutoff", null);
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

  /**
   * Returns whether the given FileReference was last modified before the modificationCutoff and, as such, should
   * be processed / published. Always returns true if there is no modificationCutoff specified (the parameter is null).
   */
  public boolean fileWithinCutoff(FileReference fileReference) {
    if (modificationCutoff == null) {
      return true;
    }

    Instant fileModifiedPlusCutoff = fileReference.lastModified.plus(modificationCutoff);
    System.out.println("returning" + fileModifiedPlusCutoff.isAfter(Instant.now()));
    return fileModifiedPlusCutoff.isBefore(Instant.now());
  }

  public URI getURI() {
    return uri;
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
