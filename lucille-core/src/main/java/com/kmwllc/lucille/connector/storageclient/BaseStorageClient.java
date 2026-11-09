package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.storageclient.StorageClientFields.ARCHIVE_FILE_SEPARATOR;
import static com.kmwllc.lucille.connector.storageclient.StorageClientFields.CONTENT;
import static com.kmwllc.lucille.connector.storageclient.StorageClientFields.FILE_PATH;
import static com.kmwllc.lucille.connector.storageclient.StorageClientFields.MODIFIED;
import static com.kmwllc.lucille.connector.storageclient.StorageClientFields.SIZE;
import static com.kmwllc.lucille.connector.storageclient.StorageClientOptions.MAX_RESULTS_PER_PAGE;
import static com.kmwllc.lucille.connector.storageclient.StorageClientOptions.MOVE_TO_AFTER_PROCESSING;
import static com.kmwllc.lucille.connector.storageclient.StorageClientOptions.MOVE_TO_ERROR_FOLDER;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.connector.StatePathTooLongException;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.filehandler.FileHandler;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.util.FileUtils;
import com.kmwllc.lucille.util.FileUtils.ContentTooLargeException;
import com.typesafe.config.Config;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Instant;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.input.CloseShieldInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.MDC.MDCCloseable;

/**
 * The base implementation for StorageClients. Must be started before traversal / getting a file's contents using
 * {@link BaseStorageClient#start()}. Call {@link BaseStorageClient#traverse(Publisher, TraversalParams)} to process and publish
 * Documents. Call {@link BaseStorageClient#getFileContentStream(URI)} to get the contents of a single file at the given URI.
 */
public abstract class BaseStorageClient implements StorageClient {

  private static final Logger log = LoggerFactory.getLogger(BaseStorageClient.class);
  private static final Logger docLogger = LoggerFactory.getLogger("com.kmwllc.lucille.core.DocLogger");

  protected final Config config;

  protected final int maxResultsPerPage;
  private boolean initialized = false;

  /**
   * Creates a base implementation of a Storage client from the given config. Validates the provided options, throwing an
   * IllegalArgumentException if they are invalid for the specific implementation.
   */
  public BaseStorageClient(Config config) {
    this(config, 100);
  }

  /**
   * Creates a base implementation of a Storage client from the given config, using the given page size when the config
   * does not set "maxResultsPerPage".
   */
  protected BaseStorageClient(Config config, int defaultMaxResultsPerPage) {
    validateOptions(config);
    this.config = config;

    // only matters for traversals
    this.maxResultsPerPage = ConfigUtils.getIntOrDefault(config, MAX_RESULTS_PER_PAGE, defaultMaxResultsPerPage);
  }

  /**
   * Validate that the given config is sufficient to construct an instance of this StorageClient.
   * Catches cases in which a config option being set requires the presence or absence of another option(s), which is not covered by
   * the Spec.
   * @throws IllegalArgumentException If the configuration is not sufficient for this StorageClient.
   */
  protected void validateOptions(Config config) { }

  // Wrapping around start, stop, and traverse to manage "initialized" before / after doing the actual operations in a
  // StorageClient-specific way.

  @Override
  public final void start() throws IOException {
    if (!initialized) {
      initializeStorageClient();
      initialized = true;
    }
  }

  // StorageClient-specific operations for start()
  protected abstract void initializeStorageClient() throws IOException;

  @Override
  public final void stop() throws IOException {
    if (initialized) {
      initialized = false;
      shutdownStorageClient();
    }
  }

  // StorageClient-specific operations for stop()
  protected abstract void shutdownStorageClient() throws IOException;

  public boolean isInitialized() {
    return this.initialized;
  }

  // Convenience method for traversing without state.
  @Override
  public final void traverse(Publisher publisher, TraversalParams params) throws Exception {
    traverse(publisher, params, null);
  }

  @Override
  public final void traverse(Publisher publisher, TraversalParams params, FileStateTracker stateMgr) throws Exception {
    if (!isInitialized()) {
      throw new IllegalStateException("This StorageClient has not been started. Call start() first.");
    }

    traverseStorageClient(publisher, params, stateMgr);
  }

  protected abstract void traverseStorageClient(Publisher publisher, TraversalParams params, FileStateTracker stateMgr) throws Exception;

  /**
   * {@inheritDoc}
   * <p> For object stores, where a path is a raw key prefix: the parent contains the child when the child's key prefix
   * starts with the parent's. So <code>s3://bucket/data</code> contains both <code>s3://bucket/data/2024</code> and
   * <code>s3://bucket/data2</code>, while <code>s3://bucket/data/</code> contains only the first. A URI with an empty
   * path is the root of its bucket, and contains everything in it.
   */
  @Override
  public boolean containsPath(URI parent, URI child) {
    URI normalizedParent = parent.normalize();
    URI normalizedChild = child.normalize();

    if (!Objects.equals(normalizedParent.getScheme(), normalizedChild.getScheme())
        || !Objects.equals(normalizedParent.getAuthority(), normalizedChild.getAuthority())) {
      return false;
    }

    return keyPrefix(normalizedChild).startsWith(keyPrefix(normalizedParent));
  }

  /**
   * Returns the object-store key a URI names: its path without the leading '/'. This is the key prefix a traversal path
   * lists, or the folder key a move uses (see {@link #destinationKey}). Empty for the root of the bucket or container,
   * with or without a trailing '/'.
   */
  static String keyPrefix(URI uri) {
    String path = uri.getPath();

    if (path == null || path.isEmpty()) {
      return "";
    }

    return path.startsWith("/") ? path.substring(1) : path;
  }

  /**
   * Implements {@link #moveTargetInsidePath(URI, URI)} for object stores that move files by {@link #destinationKey}.
   * Different buckets never overlap.
   */
  static boolean movedKeyInsideBucketPath(URI path, URI folder) {
    URI normalizedPath = path.normalize();
    URI normalizedFolder = folder.normalize();

    return Objects.equals(normalizedPath.getScheme(), normalizedFolder.getScheme())
        && Objects.equals(normalizedPath.getAuthority(), normalizedFolder.getAuthority())
        && movedKeyInsidePrefix(keyPrefix(normalizedPath), keyPrefix(normalizedFolder));
  }

  /**
   * Returns whether objects listed under the given key prefix would still be under it after moving to the given folder.
   * Every listed key starts with the prefix, so the destination of the prefix itself decides for all of them. This covers
   * a folder under the prefix, the prefix without its trailing '/' (the file lands at <code>incoming/incoming/a.csv</code>)
   * and the root of the bucket (the file lands on its own key).
   */
  static boolean movedKeyInsidePrefix(String prefix, String folderKey) {
    return destinationKey(folderKey, prefix).startsWith(prefix);
  }

  /**
   * Returns whether the provided URI, which represents a directory in some storage provider, is a directory
   * that should be skipped and not traversed at all, based on the <code>pathsToSkip</code> in <code>params</code>.
   * @param directoryURI A URI representing a directory (or the notion of a directory) in a storage provider.
   * @param params Parameters for a traversal.
   * @return Whether the directory at the provided URI ought to be skipped.
   */
  protected boolean isSkippedDirectory(URI directoryURI, TraversalParams params) {
    String normalized = ensureTrailingSlash(directoryURI.toString());
    return params.getPathsToSkip().stream()
        .anyMatch(skipDir -> ensureTrailingSlash(skipDir.toString()).equals(normalized));
  }

  private static String ensureTrailingSlash(String s) {
    return s.endsWith("/") ? s : s + "/";
  }

  @Override
  public final InputStream getFileContentStream(URI uri) throws IOException {
    if (!isInitialized()) {
      throw new IllegalStateException("This StorageClient has not been started. Call start() first.");
    }

    return getFileContentStreamFromStorage(uri);
  }

  // Performs a StorageClient-specific approach to get the contents of the file at the given URI (in an InputStream),
  protected abstract InputStream getFileContentStreamFromStorage(URI uri) throws IOException;

  /**
   * If the file is valid, it will be processed and published. Also, perform any preprocessing, error handling, and post-processing.
   */
  protected void processAndPublishFileIfValid(Publisher publisher, FileReference fileReference, TraversalParams params, FileStateTracker stateMgr) {
    URI fullPath = fileReference.getFullPath();
    String fileExtension = fileReference.getFileExtension();

    try {
      // We always mark files as encountered, even if they don't comply with filter options
      if (stateMgr != null) {
        stateMgr.markFileEncountered(fullPath.toString());
      }

      Instant lastPublished = stateMgr == null ? null : stateMgr.getLastPublished(fullPath.toString());

      // Skip files that aren't valid (e.g. directories)
      if (!fileReference.isValidFile()) {
        return;
      }

      // Skip files that don't pass the filter options
      if (!params.includeFile(fileReference.getFilterName(), fileReference.getLastModified(), lastPublished)) {
        // If this is a previously-published archive or compressed file, its entries won't be traversed
        // this run. Mark all entries as encountered so they are not incorrectly marked as tombstones
        if (stateMgr != null && lastPublished != null) {
          if ((params.getHandleArchivedFiles() && isSupportedArchiveFileType(fullPath.toString()))
              || (params.getHandleCompressedFiles() && isSupportedCompressedFileType(fullPath.toString()))) {
            stateMgr.markAllEntriesEncountered(fullPath + ARCHIVE_FILE_SEPARATOR);
          }
        }
        return;
      }

      // Skip the file if pre-processing fails.
      // (preprocessing is currently a NO-OP unless a subclass overrides it)
      if (!beforeProcessingFile(fullPath)) {
        return;
      }

      // handle compressed files if needed to the end
      if (params.getHandleCompressedFiles() && isSupportedCompressedFileType(fullPath.toString())) {
        // unzip the file, compressorStream will be closed when try block is exited
        try (BufferedInputStream bis = new BufferedInputStream(fileReference.getContentStream(params));
            CompressorInputStream compressorStream = new CompressorStreamFactory().createCompressorInputStream(bis)) {
          // we can remove the last extension from path knowing before we confirmed that it has a compressed extension
          String decompressedPath = FilenameUtils.removeExtension(fullPath.toString());
          String resolvedExtension = FilenameUtils.getExtension(decompressedPath);

          // if detected to be an archive type after decompression
          if (params.getHandleArchivedFiles() && isSupportedArchiveFileType(decompressedPath)) {
            handleArchiveFiles(publisher, compressorStream, fullPath, params, stateMgr);
          } else {
            String compressedFileFullPath = fullPath + ARCHIVE_FILE_SEPARATOR + FilenameUtils.getName(decompressedPath);

            if (stateMgr != null) {
              stateMgr.markFileEncountered(compressedFileFullPath);
            }

            // if file is a supported file type that should be handled by a file handler
            if (params.supportsFileExtension(resolvedExtension)) {
              handleStreamExtensionFiles(publisher, resolvedExtension, params, compressorStream, compressedFileFullPath);
            } else {
              Document doc = fileReference.decompressedFileAsDoc(compressorStream, compressedFileFullPath, params);
              if (docLogger.isInfoEnabled()) {
                try (MDCCloseable mdc = MDC.putCloseable(Document.ID_FIELD, doc.getId())) {
                  docLogger.info("StorageClient to publish Document {}.", doc.getId());
                }
              }
              publisher.publish(doc);
            }

            if (stateMgr != null) {
              stateMgr.successfullyPublishedFile(compressedFileFullPath);
            }
          }
        }

        // Regardless of what actually gets published in the above code, the compressed file itself was considered / processed by Lucille.
        if (stateMgr != null) {
          stateMgr.successfullyPublishedFile(fullPath.toString());
        }

        afterProcessingFile(fullPath, params);
        return;
      }

      // handle archived files if needed to the end
      if (params.getHandleArchivedFiles() && isSupportedArchiveFileType(fullPath.toString())) {
        try (InputStream is = fileReference.getContentStream(params)) {
          handleArchiveFiles(publisher, is, fullPath, params, stateMgr);
        }

        // Regardless of what is extracted & published in handleArchiveFiles, the archive file itself was considered / processed by Lucille.
        if (stateMgr != null) {
          stateMgr.successfullyPublishedFile(fullPath.toString());
        }

        afterProcessingFile(fullPath, params);
        return;
      }

      // handle file types using fileHandler if needed to the end
      if (params.supportsFileExtension(fileExtension)) {
        // Get a stream for the file content, so we don't have to load it all at once.
        InputStream contentStream = fileReference.getContentStream(params);
        // get the right FileHandler and publish based on content
        publishUsingFileHandler(publisher, fileExtension, params, contentStream, fullPath);

        if (stateMgr != null) {
          stateMgr.successfullyPublishedFile(fullPath.toString());
        }

        afterProcessingFile(fullPath, params);
        return;
      }

      // handle normal files
      Document doc = fileReference.asDoc(params);
      if (docLogger.isInfoEnabled()) {
        try (MDCCloseable mdc = MDC.putCloseable(Document.ID_FIELD, doc.getId())) {
          docLogger.info("StorageClient to publish Document {}.", doc.getId());
        }
      }
      publisher.publish(doc);

      if (stateMgr != null) {
        stateMgr.successfullyPublishedFile(fullPath.toString());
      }

      afterProcessingFile(fullPath, params);
    } catch (MoveNotSupportedException e) {
      // every file would fail the same way, so stop the traversal. Any other UnsupportedOperationException is
      // handled like other errors processing a single file, below.
      throw e;
    } catch (StatePathTooLongException e) {
      throw e;
    } catch (Exception e) {
      try {
        errorProcessingFile(fullPath, params);
      } catch (IOException ex) {
        log.error("Error occurred while performing error operations on file '{}'", fullPath, ex);
      }
      log.error("Unable to publish document '{}', SKIPPING", fullPath, e);
    }
  }

  /**
   * Returns the key an object moves to under the given folder key, for object stores: the folder, a '/', then the
   * object's full key. An empty folder key is the root of the bucket or container. A folder key ending in '/' gets no
   * second one.
   *
   * @param folderKey The destination folder's key, without a leading '/'. May be empty.
   * @param sourceKey The full key of the object being moved.
   * @return The destination key.
   */
  static String destinationKey(String folderKey, String sourceKey) {
    if (folderKey.isEmpty()) {
      return sourceKey;
    }

    return folderKey.endsWith("/") ? folderKey + sourceKey : folderKey + "/" + sourceKey;
  }

  /**
   * Builds the FileReference for one listed object, or returns null, logging the error, if it can't be built (e.g. the
   * object's name can't form a URI). The caller skips that object, so one bad object doesn't fail the whole traversal.
   * It is never published or marked encountered in state, because it has no full path to record it under. So if an
   * object with that name was published by an earlier run, its row is treated like any file not encountered: once it
   * has gone unencountered for <code>state.runsBeforeExpiration</code> runs, it is tombstoned (with
   * <code>sendTombstones</code>) and deleted from state.
   */
  static FileReference buildReferenceOrSkip(String objectName, Supplier<FileReference> builder) {
    try {
      return builder.get();
    } catch (Exception e) {
      log.error("Unable to build a reference to '{}', SKIPPING", objectName, e);
      return null;
    }
  }

  /**
   * This handles all entries of an archive file. Performs validity of an entry using includes/excludes as configured
   * in the config file before processing. Entries do not have a created date, and depending on the implementation
   * of the ArchiveInputStream, may return size as -1. Does not support recursive archives.
   *
   * @param publisher publisher used to publish documents
   * @param inputStream input stream of the archive file. Used to create an ArchiveInputStream
   * @param fullPathURI full path of the archive file including the extension. Can be a cloud path or local path.
   *                    Cloud path would include schema and bucket/container name
   *                    e.g gs://bucket-name/folder/file.zip or s3://bucket-name/file.tar
   * @param params Parameters to customize the traversal / handling of files.
   *
   * @throws ArchiveException If an error occurs getting the archive file's entries.
   * @throws IOException If an error regarding file I/O occurs
   * @throws ConnectorException If an error occurs handling subsequent archive entries.
   */
  private void handleArchiveFiles(Publisher publisher, InputStream inputStream, URI fullPathURI, TraversalParams params, FileStateTracker stateMgr) throws ArchiveException, IOException, ConnectorException {
    try (BufferedInputStream bis = new BufferedInputStream(inputStream);
        ArchiveInputStream<? extends ArchiveEntry> in = new ArchiveStreamFactory().createArchiveInputStream(bis)) {
      ArchiveEntry entry = null;

      while ((entry = in.getNextEntry()) != null) {
        String entryFullPathStr = getArchiveEntryFullPath(fullPathURI, entry.getName());

        if (stateMgr != null) {
          stateMgr.markFileEncountered(entryFullPathStr);
        }

        if (!in.canReadEntryData(entry)) {
          log.info("Cannot read entry data for entry: '{}' in '{}'. Skipping...", entry.getName(), fullPathURI);
          continue;
        }

        Instant archiveLastPublished = stateMgr == null ? null : stateMgr.getLastPublished(entryFullPathStr);
        // some archive formats and entries have no modification date
        Instant entryLastModified = entry.getLastModifiedDate() == null ? null : entry.getLastModifiedDate().toInstant();
        // checking validity only for the entries
        if (!entry.isDirectory() && params.includeFile(entry.getName(), entryLastModified, archiveLastPublished)) {
          String entryExtension = FilenameUtils.getExtension(entry.getName());
          if (params.supportsFileExtension(entryExtension)) {
            handleStreamExtensionFiles(publisher, entryExtension, params, in, entryFullPathStr);
          } else {
            // handle entry to be published as a normal document
            // note that if there exists a file within the same parent directory with the same name as the entries, it will have the same id
            Document doc = Document.create(StorageClient.createDocId(DigestUtils.md5Hex(entryFullPathStr), params));
            doc.setField(FILE_PATH, entryFullPathStr);
            if (entryLastModified != null) {
              doc.setField(MODIFIED, entryLastModified);
            }
            // entry does not have creation date
            // note that some ArchiveEntry implementations may not have a size so will return -1
            doc.setField(SIZE, entry.getSize());
            if (params.shouldGetFileContent()) {
              Long maxSizeBytes = params.getMaxSizeBytes();
              try {
                if (maxSizeBytes != null && entry.getSize() > maxSizeBytes) {
                  throw new ContentTooLargeException(
                      String.format("Entry of %d bytes exceeds maxSizeBytes of %d.", entry.getSize(), maxSizeBytes));
                }
                doc.setField(CONTENT, FileUtils.readAllBytes(in, maxSizeBytes));
              } catch (ContentTooLargeException e) {
                // skip just this entry rather than failing the whole archive
                log.error("Skipping entry '{}' in '{}'. {}", entry.getName(), fullPathURI, e.getMessage());
                continue;
              }
            }
            try {
              if (docLogger.isInfoEnabled()) {
                try (MDCCloseable mdc = MDC.putCloseable(Document.ID_FIELD, doc.getId())) {
                  docLogger.info("StorageClient to publish Document {}.", doc.getId());
                }
              }
              publisher.publish(doc);
            } catch (Exception e) {
              throw new ConnectorException("Error occurred while publishing archive entry: " + entry.getName() + " in " + fullPathURI, e);
            }
          }

          if (stateMgr != null) {
            stateMgr.successfullyPublishedFile(entryFullPathStr);
          }
        }
      }
    }
  }

  /**
   * This processes the file after it has been decompressed or extracted from an archive. This method is only called when
   * the resolved file has a supported extension and user has configured the appropriate file handler.
   *
   * @param publisher publisher used to publish documents
   * @param in An InputStream for an archive / compressed file. This InputStream will <b>NOT</b> be closed by this method.
   * @param fileExtension The extension of the file you're processing.
   * @param fullPathStr can be entry full path or decompressed full path. Can be a cloud path or local path.
   *                    e.g. gs://bucket-name/folder/file.zip!entry.json OR path/to/example.csv
   * @throws ConnectorException If an error occurs processing / handling the file.
   */
  private void handleStreamExtensionFiles(Publisher publisher, String fileExtension, TraversalParams params, InputStream in, String fullPathStr)
      throws ConnectorException {
    try {
      // Shield the archive / compressor stream from close() calls, but delegate everything else (notably bulk reads).
      InputStream wrappedNonClosingStream = CloseShieldInputStream.wrap(in);

      FileHandler handler = params.handlerForExtension(fileExtension);
      handler.processFileAndPublish(publisher, wrappedNonClosingStream, fullPathStr);
    } catch (Exception e) {
      throw new ConnectorException("Error occurred while handling / processing file: " + fullPathStr, e);
    }
  }

  /**
   * Publishes a file using a file handler and an InputStream to its contents.
   * @throws Exception If an error occurs or the file extension doesn't have a file handler to use.
   */
  private void publishUsingFileHandler(Publisher publisher, String fileExtension, TraversalParams params, InputStream inputStream, URI pathURI) throws Exception {
    FileHandler handler = params.handlerForExtension(fileExtension);
    if (handler == null) {
      throw new ConnectorException("No file handler found for file extension: " + fileExtension);
    }

    try {
      handler.processFileAndPublish(publisher, inputStream, pathURI.toString());
    } catch (Exception e) {
      throw new ConnectorException("Error occurred while processing or publishing file: " + pathURI, e);
    }
  }

  /**
   * helper method to get the full path of an entry in an archived file. Only used for archive files.
   * @param fullPathURI A URI representing the full path to the archive file.
   * @param entryName The name of the file extracted from the archive file.
   * @return A String representing the full path to this archive entry, including the full path to the
   * archive file, the archive file separator, and then the entry's name.
   */
  private String getArchiveEntryFullPath(URI fullPathURI, String entryName) {
    return fullPathURI + ARCHIVE_FILE_SEPARATOR + entryName;
  }

  /**
   * Performs preprocessing operations. Called before processing each file in a traversal.
   * @return Whether preprocessing was successful, and the file should be processed further.
   */
  private boolean beforeProcessingFile(URI filePath) throws Exception {
    // Base implementation, process all files. 
    return true;
  }

  /**
   * method for performing operations after processing files. Additional operations can be added
   * in the implementation of this method. Will be called after processing each file in traversal.
   *
   * @throws IOException If an error occurs performing post-processing operations on the file. (Namely, a <code>moveToAfterProcessing</code>
   * directory is specified and an error occurs.)
   */
  private void afterProcessingFile(URI filePath, TraversalParams params) throws IOException {
    if (params.getMoveToAfterProcessing() != null) {
      // move to processed folder
      moveFileOrFail(filePath, params.getMoveToAfterProcessing(), MOVE_TO_AFTER_PROCESSING);
    }
  }

  /**
   * method for performing operations when encountering an error while processing files. Additional operations can be added
   * in the implementation of this method. Will be called in the catch block for each file in traversal
   * in the tryProcessAndPublishFile method.
   */
  private void errorProcessingFile(URI filePath, TraversalParams params) throws IOException {
    if (params.getMoveToErrorFolder() != null) {
      // move to error folder
      moveFileOrFail(filePath, params.getMoveToErrorFolder(), MOVE_TO_ERROR_FOLDER);
    }
  }

  // Moves the file, throwing a MoveNotSupportedException, which stops the traversal, if this client doesn't support
  // moving files.
  private void moveFileOrFail(URI filePath, URI folder, String option) throws IOException {
    try {
      moveFile(filePath, folder);
    } catch (UnsupportedOperationException e) {
      throw new MoveNotSupportedException(getClass().getName() + " does not support moving files, which fileOptions."
          + option + " requires.", e);
    }
  }

  /**
   * Thrown when fileOptions.moveToAfterProcessing or fileOptions.moveToErrorFolder is set but this client's
   * {@link #moveFile(URI, URI)} throws an UnsupportedOperationException. Stops the traversal.
   */
  private static class MoveNotSupportedException extends UnsupportedOperationException {
    MoveNotSupportedException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /**
   * helper method to check if the file is a supported compressed file type.
   * note that the commented following are supported by apache-commons compress, but have yet to been tested, so commented out for now
   */
  private boolean isSupportedCompressedFileType(String pathStr) {
    return pathStr.toLowerCase(Locale.ROOT).endsWith(".gz");
    // string.endsWith(".bz2") ||
    // string.endsWith(".xz") ||
    // string.endsWith(".lzma") ||
    // string.endsWith(".br") ||
    // string.endsWith(".pack") ||
    // string.endsWith(".zst") ||
    // string.endsWith(".Z");
  }

  /**
   * helper method to check if the file is a supported archived file type.
   * note that the commented following are supported by apache-commons compress, but have yet to been tested, so commented out for now
   */
  private boolean isSupportedArchiveFileType(String pathStr) {
    String lowerCasePath = pathStr.toLowerCase(Locale.ROOT);
    return lowerCasePath.endsWith(".tar") ||
        lowerCasePath.endsWith(".zip");
    // string.endsWith(".7z") ||
    // string.endsWith(".ar") ||
    // string.endsWith(".arj") ||
    // string.endsWith(".cpio") ||
    // string.endsWith(".dump") ||
    // string.endsWith(".dmp");
  }

  /**
   * Creates a document ID using the given initial docID and the params.
   *
   * @param docId The document ID you want to use.
   * @param params Parameters for the traversal.
   * @return The document ID with the prefix added if needed / appropriate.
   */
  //should sync with abstract connector class?
  protected String createDocId(String docId, TraversalParams params) {
    return params.getDocIdPrefix() + docId;
  }

  /**
   * Should only be used for testing. Marks this storage client as initialized without calling
   * {@link #initializeStorageClient()}, for a test that supplies its own mock of the provider's SDK client.
   */
  protected void initializeForTesting() {
    this.initialized = true;
  }
}
