package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.storageclient.StorageClientOptions.GET_FILE_CONTENT;
import static com.kmwllc.lucille.connector.storageclient.StorageClientOptions.HANDLE_ARCHIVED_FILES;
import static com.kmwllc.lucille.connector.storageclient.StorageClientOptions.HANDLE_COMPRESSED_FILES;
import static com.kmwllc.lucille.connector.storageclient.StorageClientOptions.LISTING_MODE;
import static com.kmwllc.lucille.connector.storageclient.StorageClientOptions.MAX_DOCUMENTS;
import static com.kmwllc.lucille.connector.storageclient.StorageClientOptions.MAX_SIZE_BYTES;
import static com.kmwllc.lucille.connector.storageclient.StorageClientOptions.MOVE_TO_AFTER_PROCESSING;
import static com.kmwllc.lucille.connector.storageclient.StorageClientOptions.MOVE_TO_ERROR_FOLDER;
import static com.kmwllc.lucille.connector.storageclient.StorageClientOptions.THREADS_PER_PATH;
import static com.kmwllc.lucille.connector.storageclient.StorageClientOptions.TRAVERSAL;

import com.kmwllc.lucille.connector.storageclient.BaseStorageClient.ListingMode;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.filehandler.FileHandler;
import com.kmwllc.lucille.util.FileUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
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

  // a larger limit couldn't be read into a byte[]
  private static final long MAX_ALLOWED_SIZE_BYTES = Integer.MAX_VALUE - 9L;

  // provided argument
  private final String docIdPrefix;

  // The path to storage to traverse through.
  private final URI uri;

  // FileOptions
  private final boolean getFileContent;
  private final boolean handleArchivedFiles;
  private final boolean handleCompressedFiles;
  // null means no limit
  private final Long maxSizeBytes;

  private final URI moveToAfterProcessing;
  private final URI moveToErrorFolder;

  // FilterOptions
  private final List<Pattern> excludes;
  private final List<Pattern> includes;
  private final List<URI> pathsToSkip;
  private final Duration lastModifiedCutoff;
  private final Duration lastPublishedCutoff;
  private final PublishMode publishMode;

  // traversal
  private final int threadsPerPath;
  // null means no limit
  private final Long maxDocuments;
  // null means the client's default
  private final ListingMode listingMode;

  // FileHandlers
  private final Config fileHandlersConfig;
  private final Map<String, FileHandler> fileHandlers;

  public TraversalParams(Config config, URI pathToStorage, String docIdPrefix) {
    this.uri = pathToStorage;
    this.docIdPrefix = docIdPrefix;

    Config fileOptions = config.hasPath("fileOptions") ? config.getConfig("fileOptions") : ConfigFactory.empty();
    // file options / derived params
    this.getFileContent = !fileOptions.hasPath(GET_FILE_CONTENT) || fileOptions.getBoolean(GET_FILE_CONTENT);
    this.handleArchivedFiles = fileOptions.hasPath(HANDLE_ARCHIVED_FILES) && fileOptions.getBoolean(HANDLE_ARCHIVED_FILES);
    this.handleCompressedFiles = fileOptions.hasPath(HANDLE_COMPRESSED_FILES) && fileOptions.getBoolean(HANDLE_COMPRESSED_FILES);
    this.maxSizeBytes = ConfigUtils.getLongOrDefault(fileOptions, MAX_SIZE_BYTES, null);
    if (maxSizeBytes != null && maxSizeBytes <= 0) {
      throw new IllegalArgumentException("fileOptions.maxSizeBytes must be positive, but was " + maxSizeBytes);
    }
    if (maxSizeBytes != null && maxSizeBytes > MAX_ALLOWED_SIZE_BYTES) {
      throw new IllegalArgumentException("fileOptions.maxSizeBytes must be at most " + MAX_ALLOWED_SIZE_BYTES + ", but was " + maxSizeBytes);
    }

    try {
      if (fileOptions.hasPath(MOVE_TO_AFTER_PROCESSING)) {
        this.moveToAfterProcessing = FileUtils.parsePathOrURI(fileOptions.getString(MOVE_TO_AFTER_PROCESSING));
      } else {
        this.moveToAfterProcessing = null;
      }
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Error with moveToAfterProcessing URI.", e);
    }

    try {
      if (fileOptions.hasPath(MOVE_TO_ERROR_FOLDER)) {
        this.moveToErrorFolder = FileUtils.parsePathOrURI(fileOptions.getString(MOVE_TO_ERROR_FOLDER));
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

    List<String> pathsToSkipStrings = filterOptions.hasPath("pathsToSkip") ?
        filterOptions.getStringList("pathsToSkip") : Collections.emptyList();
    this.pathsToSkip = pathsToSkipStrings.stream()
        .map(TraversalParams::makePathToSkipURI)
        .collect(Collectors.toList());

    this.publishMode = filterOptions.hasPath("publishMode") ? PublishMode.fromString(filterOptions.getString("publishMode")) : PublishMode.FULL;


    this.lastModifiedCutoff = filterOptions.hasPath("lastModifiedCutoff") ? filterOptions.getDuration("lastModifiedCutoff") : null;
    this.lastPublishedCutoff = filterOptions.hasPath("lastPublishedCutoff") ? filterOptions.getDuration("lastPublishedCutoff") : null;

    // traversal options. maxConcurrentPaths is read by FileConnector, which runs the paths.
    Config traversal = config.hasPath(TRAVERSAL) ? config.getConfig(TRAVERSAL) : ConfigFactory.empty();
    this.threadsPerPath = ConfigUtils.getIntOrDefault(traversal, THREADS_PER_PATH, 1);
    if (threadsPerPath < 1) {
      throw new IllegalArgumentException(TRAVERSAL + "." + THREADS_PER_PATH + " must be at least 1, but was " + threadsPerPath);
    }
    this.maxDocuments = ConfigUtils.getLongOrDefault(traversal, MAX_DOCUMENTS, null);
    if (maxDocuments != null && maxDocuments < 1) {
      throw new IllegalArgumentException(TRAVERSAL + "." + MAX_DOCUMENTS + " must be at least 1, but was " + maxDocuments);
    }
    this.listingMode = traversal.hasPath(LISTING_MODE) ? ListingMode.fromString(traversal.getString(LISTING_MODE)) : null;

    // fileHandlers - create a map from fileExtensions to fileHandlers. The method will handle json / jsonl.
    this.fileHandlersConfig = config.hasPath("fileHandlers") ? config.getConfig("fileHandlers") : ConfigFactory.empty();
    this.fileHandlers = FileHandler.createFromConfig(fileHandlersConfig);
  }

  // Everything but the FileHandlers is immutable, and so is safe to share across threads.
  private TraversalParams(TraversalParams other) {
    this.uri = other.uri;
    this.docIdPrefix = other.docIdPrefix;
    this.getFileContent = other.getFileContent;
    this.handleArchivedFiles = other.handleArchivedFiles;
    this.handleCompressedFiles = other.handleCompressedFiles;
    this.maxSizeBytes = other.maxSizeBytes;
    this.moveToAfterProcessing = other.moveToAfterProcessing;
    this.moveToErrorFolder = other.moveToErrorFolder;
    this.excludes = other.excludes;
    this.includes = other.includes;
    this.pathsToSkip = other.pathsToSkip;
    this.lastModifiedCutoff = other.lastModifiedCutoff;
    this.lastPublishedCutoff = other.lastPublishedCutoff;
    this.publishMode = other.publishMode;
    this.threadsPerPath = other.threadsPerPath;
    this.maxDocuments = other.maxDocuments;
    this.listingMode = other.listingMode;
    this.fileHandlersConfig = other.fileHandlersConfig;
    this.fileHandlers = FileHandler.createFromConfig(fileHandlersConfig);
  }

  /**
   * Returns a copy of these params with its own FileHandler instances, for a traversal thread. A FileHandler is not
   * thread-safe, so threads must not share one.
   */
  TraversalParams withOwnFileHandlers() {
    return new TraversalParams(this);
  }

  /**
   * Returns whether the filterOptions allow for the publishing / processing of the file, described by its name
   * and the last time it was modified.
   *
   * @param fileName The name the includes / excludes patterns are matched against (see {@link FileReference#getFilterName()}).
   * @param fileLastModified When the file was last modified. May be null, in which case the file passes lastModifiedCutoff
   *                         and, in incremental mode, is treated as modified since it was last published.
   * @param fileLastPublished When the file was last published, or null if it hasn't been (or state is not in use).
   * @return Whether the file should be processed and published.
   */
  public boolean includeFile(String fileName, Instant fileLastModified, Instant fileLastPublished) {
    // file must pass include/exclude path patterns, if specified, to even be a publishing candidate
    if (!applyPatternFilters(fileName)) {
      return false;
    }

    // apply lastModified filter regardless of mode
    if (!applyLastModifiedFilter(fileLastModified)) {
      return false;
    }

    // if path is valid and publishMode is "full", publish file
    if (publishMode == PublishMode.FULL) {
      return true;
    }

    // incremental support: do not publish if file already published and not modified since
    if (!modifiedSinceLastPublish(fileLastModified, fileLastPublished)) {
      return false;
    }

    // incremental support: if lastPublishedCutoff is specified, require it to pass
    return applyLastPublishedFilter(fileLastPublished);
  }

  /**
   * @param fileExtension A file extension, matched without regard to case.
   * @return Whether a FileHandler is configured for the extension.
   */
  public boolean supportsFileExtension(String fileExtension) {
    return fileExtension != null && fileHandlers.containsKey(fileExtension.toLowerCase(Locale.ROOT));
  }

  /**
   * @param fileExtension A file extension, matched without regard to case.
   * @return The FileHandler configured for the extension, or null if there is none.
   */
  public FileHandler handlerForExtension(String fileExtension) {
    return fileExtension == null ? null : fileHandlers.get(fileExtension.toLowerCase(Locale.ROOT));
  }

  /**
   * Returns whether a file with the given name should be processed, based on the includes/excludes patterns.
   */
  private boolean applyPatternFilters(String fileName) {
    return excludes.stream().noneMatch(pattern -> pattern.matcher(fileName).matches())
        && (includes.isEmpty() || includes.stream().anyMatch(pattern -> pattern.matcher(fileName).matches()));
  }

  private boolean applyLastModifiedFilter(Instant fileLastModified) {
    // If lastModifiedCutoff is specified, return false if it is violated. A file with no last-modified time passes.
    if (lastModifiedCutoff != null && fileLastModified != null) {
      Instant cutoffPoint = Instant.now().minus(lastModifiedCutoff);

      // Only want to include files modified within the given duration (for example, within the last hour)
      if (fileLastModified.isBefore(cutoffPoint)) {
        return false;
      }
    }
    return true;
  }

  private boolean applyLastPublishedFilter(Instant fileLastPublished) {
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

  // a file with no last-modified time can't be shown to be unchanged, so it is treated as modified
  private boolean modifiedSinceLastPublish(Instant fileLastModified, Instant fileLastPublished) {
    return fileLastPublished == null || fileLastModified == null || fileLastModified.isAfter(fileLastPublished);
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

  /**
   * @return The maximum size, in bytes, of file content to read into a Document, or null if there is no limit.
   */
  public Long getMaxSizeBytes() {
    return maxSizeBytes;
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

  public List<URI> getPathsToSkip() {
    return pathsToSkip;
  }

  /**
   * @return The number of threads that traverse this path, from <code>traversal.threadsPerPath</code>. At least 1.
   */
  public int getThreadsPerPath() {
    return threadsPerPath;
  }

  /**
   * @return The most documents to publish for this path, from <code>traversal.maxDocuments</code>, or null if there is
   * no limit.
   */
  public Long getMaxDocuments() {
    return maxDocuments;
  }

  /**
   * @return The listing mode from <code>traversal.listingMode</code>, or null if it isn't set, in which case the client
   * chooses.
   */
  public ListingMode getListingMode() {
    return listingMode;
  }

  /**
   * Rejects the settings that only {@link BaseStorageClient#traverseByListing} honors: <code>threadsPerPath</code> above 1
   * and <code>listingMode</code>. For a client that walks its storage another way.
   *
   * @param client The client that would ignore them, named in the error.
   */
  void rejectListingSettings(StorageClient client) {
    if (threadsPerPath > 1) {
      throw notSupported(THREADS_PER_PATH + " above 1", client);
    }
    if (listingMode != null) {
      throw notSupported(LISTING_MODE, client);
    }
  }

  /**
   * Rejects <code>maxDocuments</code>, which only {@link BaseStorageClient} enforces. For a client that implements
   * {@link StorageClient} directly.
   *
   * @param client The client that would ignore it, named in the error.
   */
  void rejectMaxDocuments(StorageClient client) {
    if (maxDocuments != null) {
      throw notSupported(MAX_DOCUMENTS, client);
    }
  }

  private static IllegalArgumentException notSupported(String setting, StorageClient client) {
    return new IllegalArgumentException(TRAVERSAL + "." + setting + " isn't supported by " + client.getClass().getName() + ".");
  }

  /**
   * Creates a normalized URI for a path to skip.
   * <p>
   * Valid, absolute URIs are used as-is. As a convenience and a fallback, local
   * file system paths may be provided without a scheme - either absolute or relative - and are
   * converted to an absolute {@code file://} URI.
   * <p>
   * Throws an IllegalArgumentException in the event of an invalid URI / path.
   */
  private static URI makePathToSkipURI(String s) {
    try {
      URI uri = new URI(s);

      // a single-letter scheme is a Windows drive letter ("C:/data"), handled as a local path below
      if (uri.isAbsolute() && uri.getScheme().length() > 1) {
        return uri.normalize();
      }
    } catch (URISyntaxException e) {
      // Fall back, seeing if we can treat the entry as a local file system path.
    }

    // non-absolute URI, or invalid syntax: fall back to local file path.
    try {
      return Paths.get(s).toAbsolutePath().normalize().toUri();
    } catch (Exception e) {
      throw new IllegalArgumentException("Error with path in pathsToSkip: '" + s + "'.", e);
    }
  }

  public enum PublishMode {
    INCREMENTAL, FULL;

    @Override
    public String toString() {
      return this.name().toLowerCase();
    }

    public static PublishMode fromString(String name) {
      for (PublishMode mode : values()) {
        if (name != null && mode.name().equalsIgnoreCase(name.trim())) {
          return mode;
        }
      }
      throw new IllegalArgumentException("Unknown filterOptions.publishMode \"" + name + "\"; expected one of incremental, full");
    }
  }

}
