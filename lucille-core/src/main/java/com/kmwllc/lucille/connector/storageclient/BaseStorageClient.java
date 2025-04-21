package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.FileConnector.CONTENT;
import static com.kmwllc.lucille.connector.FileConnector.FILE_PATH;
import static com.kmwllc.lucille.connector.FileConnector.MAX_NUM_OF_PAGES;
import static com.kmwllc.lucille.connector.FileConnector.MODIFIED;
import static com.kmwllc.lucille.connector.FileConnector.SIZE;
import static com.kmwllc.lucille.connector.FileConnector.ARCHIVE_FILE_SEPARATOR;

import com.kmwllc.lucille.connector.FileConnectorStateManager;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.fileHandler.FileHandler;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.MDC.MDCCloseable;

public abstract class BaseStorageClient implements StorageClient {

  private static final Logger log = LoggerFactory.getLogger(BaseStorageClient.class);
  private static final Logger docLogger = LoggerFactory.getLogger("com.kmwllc.lucille.core.DocLogger");

  protected final Config config;
  protected final int maxNumOfPages;

  private Map<String, FileHandler> fileHandlers;

  private boolean initialized = false;

  public BaseStorageClient(Config config) {
    validateOptions(config);
    this.config = config;
    this.fileHandlers = new HashMap<>();

    // only matters for traversals
    this.maxNumOfPages = config.hasPath(MAX_NUM_OF_PAGES) ? config.getInt(MAX_NUM_OF_PAGES) : 100;
  }

  /**
   * Validate that the given config is sufficient to construct an instance of this StorageClient. Throws an
   * IllegalArgumentException if the config does not contain the necessary information.
   */
  protected abstract void validateOptions(Config config);

  @Override
  public final void init() throws IOException {
    if (!initialized) {
      initializeStorageClient();
      initialized = true;
    }
  }

  // Actually do the storage client specific initialization.
  protected abstract void initializeStorageClient() throws IOException;

  @Override
  public final void shutdown() throws IOException {
    if (initialized) {
      initialized = false;
      shutdownStorageClient();
    }
  }

  // Actually do the storage client specific shutdown.
  protected abstract void shutdownStorageClient() throws IOException;

  /**
   * Returns whether this StorageClient is currently initialized successfully and not shutdown.
   */
  public boolean isInitialized() {
    return this.initialized;
  }

  // Convenience method for traversing without state.
  @Override
  public final void traverse(Publisher publisher, TraversalParams params) throws Exception {
    traverse(publisher, params, null);
  }

  @Override
  public final void traverse(Publisher publisher, TraversalParams params, FileConnectorStateManager stateMgr) throws Exception {
    if (!isInitialized()) {
      throw new IllegalStateException("This StorageClient has not been initialized.");
    }

    try {
      this.fileHandlers = FileHandler.createFromConfig(params.getFileOptions());
      traverseStorageClient(publisher, params, stateMgr);
    } finally {
      fileHandlers = null;
    }
  }

  protected abstract void traverseStorageClient(Publisher publisher, TraversalParams params, FileConnectorStateManager stateMgr) throws Exception;

  @Override
  public final InputStream getFileContentStream(URI uri) throws IOException {
    if (!isInitialized()) {
      throw new IllegalStateException("This StorageClient has not been initialized.");
    }

    return getFileContentStreamFromStorage(uri);
  }

  protected abstract InputStream getFileContentStreamFromStorage(URI uri) throws IOException;

  /**
   * This method would try to process and publish the file. It also performs any preprocessing, error handling, and post-processing.
   *
   * @param publisher publisher used to publish documents
   * @param fileReference fileReference object that contains the Path for local Storage or Storage Item implementation for cloud storage
   */
  protected void processAndPublishFileIfValid(Publisher publisher, FileReference fileReference, TraversalParams params, FileConnectorStateManager stateMgr) {
    // Skip the file if it's not valid (a directory), and don't mark it as encountered / do anything involving state.
    if (!fileReference.isValidFile()) {
      return;
    }

    String fullPathStr = fileReference.getFullPath();
    String fileExtension = fileReference.getFileExtension();

    try {
      // We always mark files as encountered, even if they don't comply with filter options
      if (stateMgr != null) {
        stateMgr.markFileEncountered(fullPathStr);
      }

      Instant lastPublished = stateMgr == null ? null : stateMgr.getLastPublished(fullPathStr);

      // applying filter options + making sure preprocessing is successful.
      if (!params.includeFile(fileReference.getName(), fileReference.getLastModified(), lastPublished)
          || !beforeProcessingFile(fullPathStr)) {
        return;
      }

      // handle compressed files if needed to the end
      if (params.getHandleCompressedFiles() && isSupportedCompressedFileType(fullPathStr)) {
        // unzip the file, compressorStream will be closed when try block is exited
        try (BufferedInputStream bis = new BufferedInputStream(fileReference.getContentStream(params));
            CompressorInputStream compressorStream = new CompressorStreamFactory().createCompressorInputStream(bis)) {
          // we can remove the last extension from path knowing before we confirmed that it has a compressed extension
          String decompressedPath = FilenameUtils.removeExtension(fullPathStr);

          // if detected to be an archive type after decompression
          if (params.getHandleArchivedFiles() && isSupportedArchiveFileType(decompressedPath)) {
            handleArchiveFiles(publisher, compressorStream, fullPathStr, params, stateMgr);
          } else {
            String compressedFileFullPath = getArchiveEntryFullPath(fullPathStr, FilenameUtils.getName(decompressedPath));
            if (stateMgr != null) {
              stateMgr.markFileEncountered(compressedFileFullPath);
            }

            // if file is a supported file type that should be handled by a file handler
            String resolvedExtension = FilenameUtils.getExtension(decompressedPath);

            if (params.supportedFileType(resolvedExtension)) {
              handleStreamExtensionFiles(publisher, compressorStream, resolvedExtension, compressedFileFullPath);
            } else {
              Document doc = fileReference.asDoc(compressorStream, compressedFileFullPath, params);
              try (MDCCloseable mdc = MDC.putCloseable(Document.ID_FIELD, doc.getId())) {
                docLogger.info("StorageClient to publish Document {}.", doc.getId());
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
          stateMgr.successfullyPublishedFile(fullPathStr);
        }
        afterProcessingFile(fullPathStr, params);
        return;
      }

      // handle archived files if needed to the end
      if (params.getHandleArchivedFiles() && isSupportedArchiveFileType(fullPathStr)) {
        try (InputStream is = fileReference.getContentStream(params)) {
          handleArchiveFiles(publisher, is, fullPathStr, params, stateMgr);
        }

        // Regardless of what is extracted & published in handleArchiveFiles, the archive file itself was considered / processed by Lucille.
        if (stateMgr != null) {
          stateMgr.successfullyPublishedFile(fullPathStr);
        }
        afterProcessingFile(fullPathStr, params);
        return;
      }

      // handle file types using fileHandler if needed to the end
      if (params.supportedFileType(fileExtension)) {
        // Get a stream for the file content, so we don't have to load it all at once.
        InputStream contentStream = fileReference.getContentStream(params);
        // get the right FileHandler and publish based on content.
        publishUsingFileHandler(publisher, fileExtension, contentStream, fullPathStr);

        if (stateMgr != null) {
          stateMgr.successfullyPublishedFile(fullPathStr);
        }

        afterProcessingFile(fullPathStr, params);
        return;
      }

      // handle normal files
      Document doc = fileReference.asDoc(params);
      try (MDCCloseable mdc = MDC.putCloseable(Document.ID_FIELD, doc.getId())) {
        docLogger.info("StorageClient to publish Document {}.", doc.getId());
      }
      publisher.publish(doc);

      if (stateMgr != null) {
        stateMgr.successfullyPublishedFile(fullPathStr);
      }

      afterProcessingFile(fullPathStr, params);
    } catch (UnsupportedOperationException e) {
      throw new UnsupportedOperationException("Encountered unsupported operation", e);
    } catch (Exception e) {
      try {
        errorProcessingFile(fullPathStr, params);
      } catch (IOException ex) {
        log.error("Error occurred while performing error operations on file '{}'", fullPathStr, ex);
      }
      log.error("Unable to publish document '{}', SKIPPING", fullPathStr, e);
    }
  }

  /**
   * This handles all entries of an archive file. Performs validity of an entry using includes/excludes as configured
   * in the config file before processing. Entries do not have a created date, and depending on the implementation
   * of the ArchiveInputStream, may return size as -1. Does not support recursive archives.
   *
   * @param publisher publisher used to publish documents
   * @param inputStream input stream of the archive file. Used to create an ArchiveInputStream
   * @param fullPathStr full path of the archive file including the extension. Can be a cloud path or local path.
   *                    Cloud path would include schema and bucket/container name
   *                    e.g gs://bucket-name/folder/file.zip or s3://bucket-name/file.tar
   */
  private void handleArchiveFiles(Publisher publisher, InputStream inputStream, String fullPathStr, TraversalParams params, FileConnectorStateManager stateMgr) throws ArchiveException, IOException, ConnectorException {
    try (BufferedInputStream bis = new BufferedInputStream(inputStream);
        ArchiveInputStream<? extends ArchiveEntry> in = new ArchiveStreamFactory().createArchiveInputStream(bis)) {
      ArchiveEntry entry = null;

      while ((entry = in.getNextEntry()) != null) {
        String entryFullPathStr = getArchiveEntryFullPath(fullPathStr, entry.getName());
        if (stateMgr != null) {
          stateMgr.markFileEncountered(entryFullPathStr);
        }

        if (!in.canReadEntryData(entry)) {
          log.info("Cannot read entry data for entry: '{}' in '{}'. Skipping...", entry.getName(), fullPathStr);
          continue;
        }

        Instant archiveLastPublished = stateMgr == null ? null : stateMgr.getLastPublished(entryFullPathStr);
        // checking validity only for the entries
        if (!entry.isDirectory() && params.includeFile(entry.getName(), entry.getLastModifiedDate().toInstant(), archiveLastPublished)) {
          String entryExtension = FilenameUtils.getExtension(entry.getName());

          if (params.supportedFileType(entryExtension)) {
            handleStreamExtensionFiles(publisher, in, entryExtension, entryFullPathStr);
          } else {
            // handle entry to be published as a normal document
            // note that if there exists a file within the same parent directory with the same name as the entries, it will have the same id
            Document doc = Document.create(StorageClient.createDocId(DigestUtils.md5Hex(entryFullPathStr), params));
            doc.setField(FILE_PATH, entryFullPathStr);
            doc.setField(MODIFIED, entry.getLastModifiedDate().toInstant());
            // entry does not have creation date
            // note that some ArchiveEntry implementations may not have a size so will return -1
            doc.setField(SIZE, entry.getSize());
            if (params.shouldGetFileContent()) {
              doc.setField(CONTENT, in.readAllBytes());
            }
            try {
              try (MDCCloseable mdc = MDC.putCloseable(Document.ID_FIELD, doc.getId())) {
                docLogger.info("StorageClient to publish Document {}.", doc.getId());
              }
              publisher.publish(doc);
            } catch (Exception e) {
              throw new ConnectorException("Error occurred while publishing archive entry: " + entry.getName() + " in " + fullPathStr, e);
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
   * @param fullPathStr can be entry full path or decompressed full path. Can be a cloud path or local path.
   *                    e.g. gs://bucket-name/folder/file.zip!entry.json OR path/to/example.csv
   */
  private void handleStreamExtensionFiles(Publisher publisher, InputStream in, String fileExtension, String fullPathStr)
      throws ConnectorException {
    try {
      InputStream wrappedNonClosingStream = new InputStream() {
        @Override
        public int read() throws IOException {
          return in.read();
        }

        // Intentionally a no-op. We don't want to close the archiveInputStream when finished
        // with this one file.
        @Override
        public void close() {}
      };

      FileHandler handler = fileHandlers.get(fileExtension);
      handler.processFileAndPublish(publisher, wrappedNonClosingStream, fullPathStr);
    } catch (Exception e) {
      throw new ConnectorException("Error occurred while handling / processing file: " + fullPathStr, e);
    }
  }

  /**
   * Helper method to publish via file handler, using an InputStream for the file's contents.
   */
  private void publishUsingFileHandler(Publisher publisher, String fileExtension, InputStream inputStream, String fullPathStr) throws Exception {
    FileHandler handler = fileHandlers.get(fileExtension);
    if (handler == null) {
      throw new ConnectorException("No file handler found for file extension: " + fileExtension);
    }

    try {
      handler.processFileAndPublish(publisher, inputStream, fullPathStr);
    } catch (Exception e) {
      throw new ConnectorException("Error occurred while processing or publishing file: " + fullPathStr, e);
    }
  }

  /**
   * helper method to get the full path of an entry in an archived file. Only used for archive files
   */
  private String getArchiveEntryFullPath(String fullPathStr, String entryName) {
    return fullPathStr + ARCHIVE_FILE_SEPARATOR + entryName;
  }

  /**
   * method for performing operations before processing files. This method is ill be called before processing 
   * each file in traversal.  If the method returns true, the file will be processed.  A return of false indicates
   * the file should be skipped.
   */
  private boolean beforeProcessingFile(String pathStr) throws Exception {
    // Base implementation, process all files. 
    return true;
  }

  /**
   * method for performing operations after processing files. Additional operations can be added
   * in the implementation of this method. Will be called after processing each file in traversal.
   */
  private void afterProcessingFile(String pathStr, TraversalParams params) throws IOException {
    if (params.getMoveToAfterProcessing() != null) {
      // move to processed folder
      moveFile(pathStr, params.getMoveToAfterProcessing());
    }
  }

  /**
   * method for performing operations when encountering an error while processing files. Additional operations can be added
   * in the implementation of this method. Will be called in the catch block for each file in traversal
   * in the tryProcessAndPublishFile method.
   */
  private void errorProcessingFile(String pathStr, TraversalParams params) throws IOException {
    if (params.getMoveToErrorFolder() != null) {
      // move to error folder
      moveFile(pathStr, params.getMoveToErrorFolder());
    }
  }

  private void moveFile(String pathStr, String option) throws IOException {
    if (pathStr.startsWith("classpath:")) {
      log.warn("Skipping moving classpath file: {} to {}", pathStr, option);
      return;
    }

    // get paths of source and target
    Path sourcePath = Paths.get(pathStr);
    Path targetFolderPath = Paths.get(option);

    // ensure target folder exists as a precaution, only create if source path is local, meaning it is not a cloud path
    if (Files.exists(sourcePath) && !Files.exists(targetFolderPath)) {
      Files.createDirectory(targetFolderPath);
    }

    if (Files.exists(sourcePath)) {
      // move the local file to the target folder
      Path targetPath = targetFolderPath.resolve(sourcePath.getFileName());
      Files.move(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
      log.debug("Moved local file to: {}", targetPath);
    } else {
      // handle cloud paths
      throw new UnsupportedOperationException("Moving cloud files is not supported yet");
    }
  }

  /**
   * helper method to check if the file is a supported compressed file type.
   * note that the commented following are supported by apache-commons compress, but have yet to been tested, so commented out for now
   */
  private boolean isSupportedCompressedFileType(String pathStr) {
    return pathStr.endsWith(".gz");
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
    return pathStr.endsWith(".tar") ||
        pathStr.endsWith(".zip");
    // string.endsWith(".7z") ||
    // string.endsWith(".ar") ||
    // string.endsWith(".arj") ||
    // string.endsWith(".cpio") ||
    // string.endsWith(".dump") ||
    // string.endsWith(".dmp");
  }

  // Only for testing
  void initializeForTesting() {
    this.initialized = true;
  }
}
