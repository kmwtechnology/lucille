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
import java.net.URISyntaxException;
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

  private final URI moveToAfterProcessing;
  private final URI moveToErrorFolder;

  // FilterOptions / its params
  private final List<Pattern> excludes;
  private final List<Pattern> includes;
  private final Duration modificationCutoff;

  /**
   * Creates TraversalParams representing the provided options.
   *
   * @throws IllegalArgumentException If fileOptions.moveToAfterProcessing or fileOptions.moveToErrorFolder are specified but are not
   * valid URIs.
   */
  public TraversalParams(URI uri, String docIdPrefix, Config fileOptions, Config filterOptions) {
    this.uri = uri;
    this.docIdPrefix = docIdPrefix;

    // file options / derived params
    this.fileOptions = fileOptions;
    this.getFileContent = !fileOptions.hasPath(GET_FILE_CONTENT) || fileOptions.getBoolean(GET_FILE_CONTENT);
    this.handleArchivedFiles = fileOptions.hasPath(HANDLE_ARCHIVED_FILES) && fileOptions.getBoolean(HANDLE_ARCHIVED_FILES);
    this.handleCompressedFiles = fileOptions.hasPath(HANDLE_COMPRESSED_FILES) && fileOptions.getBoolean(HANDLE_COMPRESSED_FILES);

    try {
      if (fileOptions.hasPath("moveToAfterProcessing")) {
        this.moveToAfterProcessing = new URI(fileOptions.getString("moveToAfterProcessing"));
      } else {
        this.moveToAfterProcessing = null;
      }
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Error with moveToAfterProcessing URI.", e);
    }

    try {
      if (fileOptions.hasPath("moveToErrorFolder")) {
        this.moveToErrorFolder = new URI(fileOptions.getString("moveToErrorFolder"));
      } else {
        this.moveToErrorFolder = null;
      }
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Error with moveToErrorFolder URI.", e);
    }

    // filter options / derived params
    List<String> includeRegex = filterOptions.hasPath("includes") ?
        filterOptions.getStringList("includes") : Collections.emptyList();
    this.includes = includeRegex.stream().map(Pattern::compile).collect(Collectors.toList());
    List<String> excludeRegex = filterOptions.hasPath("excludes") ?
        filterOptions.getStringList("excludes") : Collections.emptyList();
    this.excludes = excludeRegex.stream().map(Pattern::compile).collect(Collectors.toList());

    this.modificationCutoff = filterOptions.hasPath("modificationCutoff") ? filterOptions.getDuration("modificationCutoff") : null;
  }

  /**
   * Returns whether the filterOptions allow for the publishing / processing of the file, described by its name
   * and the last time it was modified.
   */
  public boolean includeFile(String fileName, Instant fileLastModified) {
    return patternsAllowFile(fileName) && timeWithinCutoff(fileLastModified);
  }

  /**
   * Returns whether a file with the given name should be processed, based on the includes/excludes patterns.
   */
  private boolean patternsAllowFile(String fileName) {
    return excludes.stream().noneMatch(pattern -> pattern.matcher(fileName).matches())
        && (includes.isEmpty() || includes.stream().anyMatch(pattern -> pattern.matcher(fileName).matches()));
  }

  /**
   * Returns whether the given Instant indicates the file should be published / processed. Always returns true if there is no
   * modificationCutoff set. If it is set, returns whether the file was modified recently enough to be processed/published.
   */
  private boolean timeWithinCutoff(Instant fileLastModified) {
    if (modificationCutoff == null) {
      return true;
    }

    Instant cutoffPoint = Instant.now().minus(modificationCutoff);
    return fileLastModified.isAfter(cutoffPoint);
  }

  /**
   * Returns whether a file with the given extension is supported, as per these TraversalParams. Handles the nuance
   * of json supporting jsonl and vice versa.
   */
  public boolean supportedFileType(String fileExtension) {
    return !getFileOptions().isEmpty() && FileHandler.supportAndContainFileType(fileExtension, getFileOptions());
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

  public URI getMoveToAfterProcessing() {
    return moveToAfterProcessing;
  }

  public URI getMoveToErrorFolder() {
    return moveToErrorFolder;
  }
}
