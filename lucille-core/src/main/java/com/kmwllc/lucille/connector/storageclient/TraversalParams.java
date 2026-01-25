package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.FileConnector.GET_FILE_CONTENT;
import static com.kmwllc.lucille.connector.FileConnector.HANDLE_ARCHIVED_FILES;
import static com.kmwllc.lucille.connector.FileConnector.HANDLE_COMPRESSED_FILES;

import com.kmwllc.lucille.connector.FileConnector;
import com.kmwllc.lucille.core.fileHandler.FileHandler;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The arguments / parameters associated with a traversal on a StorageClient.
 */
public class TraversalParams {

  private static final Logger log = LoggerFactory.getLogger(TraversalParams.class);

  // provided argument
  private final String docIdPrefix;

  // The path to storage to traverse through.
  private final URI uri;

  // FileOptions
  private final boolean getFileContent;
  private final boolean handleArchivedFiles;
  private final boolean handleCompressedFiles;

  private final URI moveToAfterProcessing;
  private final URI moveToErrorFolder;

  // FilterOptions
  private final List<Pattern> excludes;
  private final List<Pattern> includes;
  private final Duration lastModifiedCutoff;
  private final Duration lastPublishedCutoff;

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

    try {
      if (fileOptions.hasPath(FileConnector.MOVE_TO_AFTER_PROCESSING)) {
        this.moveToAfterProcessing = new URI(fileOptions.getString(FileConnector.MOVE_TO_AFTER_PROCESSING));
      } else {
        this.moveToAfterProcessing = null;
      }
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Error with moveToAfterProcessing URI.", e);
    }

    try {
      if (fileOptions.hasPath(FileConnector.MOVE_TO_ERROR_FOLDER)) {
        this.moveToErrorFolder = new URI(fileOptions.getString(FileConnector.MOVE_TO_ERROR_FOLDER));
      } else {
        this.moveToErrorFolder = null;
      }
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Error with moveToErrorFolder URI.", e);
    }

    // filter options / derived params
    Config filterOptions = config.hasPath("filterOptions") ? config.getConfig("filterOptions") : ConfigFactory.empty();

    List<String> includeRegex = filterOptions.hasPath("includes") ?
        filterOptions.getStringList("includes") : Collections.emptyList();
    this.includes = includeRegex.stream().map(Pattern::compile).collect(Collectors.toList());
    List<String> excludeRegex = filterOptions.hasPath("excludes") ?
        filterOptions.getStringList("excludes") : Collections.emptyList();
    this.excludes = excludeRegex.stream().map(Pattern::compile).collect(Collectors.toList());

    this.lastModifiedCutoff = filterOptions.hasPath("lastModifiedCutoff") ? filterOptions.getDuration("lastModifiedCutoff") : null;
    this.lastPublishedCutoff = filterOptions.hasPath("lastPublishedCutoff") ? filterOptions.getDuration("lastPublishedCutoff") : null;

    // fileHandlers - create a map from fileExtensions to fileHandlers. The method will handle json / jsonl.
    Config fileHandlersConfig = config.hasPath("fileHandlers") ? config.getConfig("fileHandlers") : ConfigFactory.empty();
    this.fileHandlers = FileHandler.createFromConfig(fileHandlersConfig);
  }

  /**
   * Returns whether the filterOptions allow for the publishing / processing of the file, described by its name
   * and the last time it was modified.
   */
  public boolean includeFile(String fileName, Instant fileLastModified, Instant fileLastPublished) {
    // file mush pass include/exclude path patterns, if specified, to even be a publishing candidate
    if (!applyPatternFilters(fileName)) {
      return false;
    }

    // incremental support: do not publish if 1) file already published and 2) it hasn't been modified since
    if (fileLastPublished != null && !fileLastModified.isAfter(fileLastPublished)) {
      return false;
    }

    // incremental support: publish file unless a timestamp filter is triggered
    return applyTimestampFilters(fileLastModified, fileLastPublished);
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
  private boolean applyPatternFilters(String fileName) {
    return excludes.stream().noneMatch(pattern -> pattern.matcher(fileName).matches())
        && (includes.isEmpty() || includes.stream().anyMatch(pattern -> pattern.matcher(fileName).matches()));
  }

  /**
   * Returns whether the given lastModified and lastPublished instants comply with lastModifiedCutoff / lastPublishedCutoff,
   * if they are specified.
   */
  private boolean applyTimestampFilters(Instant fileLastModified, Instant fileLastPublished) {
    // If lastModifiedCutoff is specified, return false if it is violated
    if (lastModifiedCutoff != null) {
      Instant cutoffPoint = Instant.now().minus(lastModifiedCutoff);

      // Only want to include files modified within the given duration (for example, within the last hour)
      if (fileLastModified.isBefore(cutoffPoint)) {
        return false;
      }
    }

    // If lastPublishedCutoff is specified, and we found a lastPublished Instant for the file, return false if it is violated
    if (lastPublishedCutoff != null && fileLastPublished != null) {
      Instant cutoffPoint = Instant.now().minus(lastPublishedCutoff);

      // Only want to include files last published *more* than the duration ago (for example, more than one hour ago)
      if (fileLastPublished.isAfter(cutoffPoint)) {
        return false;
      }
    }

    return true;
  }

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

  public URI getMoveToAfterProcessing() {
    return moveToAfterProcessing;
  }

  public URI getMoveToErrorFolder() {
    return moveToErrorFolder;
  }

}
