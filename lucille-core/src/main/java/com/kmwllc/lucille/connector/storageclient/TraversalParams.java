package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.FileConnector.GET_FILE_CONTENT;
import static com.kmwllc.lucille.connector.FileConnector.HANDLE_ARCHIVED_FILES;
import static com.kmwllc.lucille.connector.FileConnector.HANDLE_COMPRESSED_FILES;
import static com.kmwllc.lucille.connector.FileConnector.MOVE_TO_AFTER_PROCESSING;
import static com.kmwllc.lucille.connector.FileConnector.MOVE_TO_ERROR_FOLDER;

import com.kmwllc.lucille.core.fileHandler.FileHandler;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * The arguments / parameters associated with a traversal on a StorageClient.
 */
public class TraversalParams {

  // provided argument
  private final String docIdPrefix;

  // The path to storage to traverse through.
  private final URI uri;

  // FileOptions
  private final boolean getFileContent;
  private final boolean handleArchivedFiles;
  private final boolean handleCompressedFiles;
  private final String moveToAfterProcessing;
  private final String moveToErrorFolder;

  // FilterOptions
  private final List<Pattern> excludes;
  private final List<Pattern> includes;
  private final Duration modificationCutoff;

  // FileHandlers
  private final Map<String, FileHandler> fileHandlers;

  public TraversalParams(Config config, URI pathToStorage, String docIdPrefix) {
    this.uri = pathToStorage;
    this.docIdPrefix = docIdPrefix;

    Config fileOptions = config.hasPath("fileOptions") ? config.getConfig("fileOptions") : ConfigFactory.empty();
    // file options / derived params
    this.getFileContent = !fileOptions.hasPath(GET_FILE_CONTENT) || fileOptions.getBoolean(GET_FILE_CONTENT);
    this.handleArchivedFiles = fileOptions.hasPath(HANDLE_ARCHIVED_FILES) && fileOptions.getBoolean(HANDLE_ARCHIVED_FILES);
    this.handleCompressedFiles = fileOptions.hasPath(HANDLE_COMPRESSED_FILES) && fileOptions.getBoolean(HANDLE_COMPRESSED_FILES);
    this.moveToAfterProcessing = fileOptions.hasPath(MOVE_TO_AFTER_PROCESSING) ? fileOptions.getString(MOVE_TO_AFTER_PROCESSING) : null;
    this.moveToErrorFolder = fileOptions.hasPath(MOVE_TO_ERROR_FOLDER) ? fileOptions.getString(MOVE_TO_ERROR_FOLDER) : null;

    // filter options / derived params
    Config filterOptions = config.hasPath("filterOptions") ? config.getConfig("filterOptions") : ConfigFactory.empty();

    List<String> includeRegex = filterOptions.hasPath("includes") ?
        filterOptions.getStringList("includes") : Collections.emptyList();
    this.includes = includeRegex.stream().map(Pattern::compile).collect(Collectors.toList());
    List<String> excludeRegex = filterOptions.hasPath("excludes") ?
        filterOptions.getStringList("excludes") : Collections.emptyList();
    this.excludes = excludeRegex.stream().map(Pattern::compile).collect(Collectors.toList());

    this.modificationCutoff = filterOptions.hasPath("modificationCutoff") ? filterOptions.getDuration("modificationCutoff") : null;

    // fileHandlers - create a map from fileExtensions to fileHandlers. The method will handle json / jsonl.
    Config fileHandlersConfig = config.hasPath("fileHandlers") ? config.getConfig("fileHandlers") : ConfigFactory.empty();
    this.fileHandlers = FileHandler.createFromConfig(fileHandlersConfig);
  }

  /**
   * Returns whether the filterOptions allow for the publishing / processing of the file, described by its name
   * and the last time it was modified.
   */
  public boolean includeFile(String fileName, Instant fileLastModified) {
    return patternsAllowFile(fileName) && timeWithinCutoff(fileLastModified);
  }

  public boolean supportedFileExtension(String fileExtension) {
    return fileHandlers.containsKey(fileExtension);
  }

  public FileHandler handlerForExtension(String fileExtension) {
    return fileHandlers.get(fileExtension);
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

  /** Returns a URI to the path to storage we are traversing through. */
  public URI getURI() {
    return uri;
  }

  public String getDocIdPrefix() {
    return docIdPrefix;
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
