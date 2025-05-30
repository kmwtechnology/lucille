package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.FileConnector.GET_FILE_CONTENT;
import static com.kmwllc.lucille.connector.FileConnector.HANDLE_ARCHIVED_FILES;
import static com.kmwllc.lucille.connector.FileConnector.HANDLE_COMPRESSED_FILES;
import static com.kmwllc.lucille.connector.FileConnector.MOVE_TO_AFTER_PROCESSING;
import static com.kmwllc.lucille.connector.FileConnector.MOVE_TO_ERROR_FOLDER;

import com.kmwllc.lucille.core.fileHandler.FileHandler;
import com.typesafe.config.Config;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The arguments / parameters associated with a traversal on a StorageClient.
 */
public class TraversalParams {

  private static final Logger log = LoggerFactory.getLogger(TraversalParams.class);
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
  private final Duration lastModifiedCutoff;
  private final Duration lastPublishedCutoff;

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

    this.lastModifiedCutoff = filterOptions.hasPath("lastModifiedCutoff") ? filterOptions.getDuration("lastModifiedCutoff") : null;
    this.lastPublishedCutoff = filterOptions.hasPath("lastPublishedCutoff") ? filterOptions.getDuration("lastPublishedCutoff") : null;
  }

  /**
   * Returns whether the filterOptions allow for the publishing / processing of the file, described by its name
   * and the last time it was modified.
   */
  public boolean includeFile(String fileName, Instant fileLastModified, Instant fileLastPublished) {
    return patternsAllowFile(fileName) && cutoffsAllowFile(fileLastModified, fileLastPublished);
  }

  /**
   * Returns whether a file with the given name should be processed, based on the includes/excludes patterns.
   */
  private boolean patternsAllowFile(String fileName) {
    return excludes.stream().noneMatch(pattern -> pattern.matcher(fileName).matches())
        && (includes.isEmpty() || includes.stream().anyMatch(pattern -> pattern.matcher(fileName).matches()));
  }

  /**
   * Returns whether the given lastModified and lastPublished instants comply with lastModifiedCutoff / lastPublishedCutoff,
   * if they are specified.
   */
  private boolean cutoffsAllowFile(Instant fileLastModified, Instant fileLastPublished) {
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

  public String getMoveToAfterProcessing() {
    return moveToAfterProcessing;
  }

  public String getMoveToErrorFolder() {
    return moveToErrorFolder;
  }
}
