package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.FileConnector.CONTENT;
import static com.kmwllc.lucille.connector.FileConnector.FILE_PATH;
import static com.kmwllc.lucille.connector.FileConnector.MAX_NUM_OF_PAGES;
import static com.kmwllc.lucille.connector.FileConnector.MODIFIED;
import static com.kmwllc.lucille.connector.FileConnector.SIZE;
import static com.kmwllc.lucille.connector.FileConnector.ARCHIVE_FILE_SEPARATOR;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.fileHandler.FileHandler;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
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

/**
 * The base implementation for StorageClients. Must be initialized before traversal / getting a file's contents using
 * {@link BaseStorageClient#init()}. Call {@link BaseStorageClient#traverse(Publisher, TraversalParams)} to process and publish
 * Documents. Call {@link BaseStorageClient#getFileContentStream(URI)} to get the contents of a single file at the given URI.
 */
public abstract class BaseStorageClient implements StorageClient {

  private static final Logger log = LoggerFactory.getLogger(BaseStorageClient.class);
  private static final Logger docLogger = LoggerFactory.getLogger("com.kmwllc.lucille.core.DocLogger");

  protected final Config config;

  protected final int maxNumOfPages;
  private boolean initialized = false;

  /**
   * Creates a base implementation of a Storage client from the given config. Validates the provided options, throwing an
   * IllegalArgumentException if they are invalid for the specific implementation.
   */
  public BaseStorageClient(Config config) {
    validateOptions(config);
    this.config = config;

    // only matters for traversals
    this.maxNumOfPages = config.hasPath(MAX_NUM_OF_PAGES) ? config.getInt(MAX_NUM_OF_PAGES) : 100;
  }

  /**
   * Validate that the given config is sufficient to construct an instance of this StorageClient.
   * @throws IllegalArgumentException If the configuration is not sufficient for this StorageClient.
   */
  protected abstract void validateOptions(Config config);

  // Wrapping around init, shutdown, and traverse to manage "initialized" before / after doing the actual operations in a
  // StorageClient-specific way.

  @Override
  public final void init() throws IOException {
    if (!initialized) {
      initializeStorageClient();
      initialized = true;
    }
  }

  // StorageClient-specific initialization operations
  protected abstract void initializeStorageClient() throws IOException;

  @Override
  public final void shutdown() throws IOException {
    if (initialized) {
      initialized = false;
      shutdownStorageClient();
    }
  }

  // StorageClient-specific shutdown operations
  protected abstract void shutdownStorageClient() throws IOException;

  public boolean isInitialized() {
    return this.initialized;
  }

  @Override
  public final void traverse(Publisher publisher, TraversalParams params) throws Exception {
    if (!isInitialized()) {
      throw new IllegalStateException("This StorageClient has not been initialized.");
    }

    traverseStorageClient(publisher, params);
  }

  protected abstract void traverseStorageClient(Publisher publisher, TraversalParams params) throws Exception;

  @Override
  public final InputStream getFileContentStream(URI uri) throws IOException {
    if (!isInitialized()) {
      throw new IllegalStateException("This StorageClient has not been initialized.");
    }

    return getFileContentStreamFromStorage(uri);
  }

  // Performs a StorageClient-specific approach to get the contents of the file at the given URI (in an InputStream),
  protected abstract InputStream getFileContentStreamFromStorage(URI uri) throws IOException;

  /**
   * If the file is valid, it will be processed and published. Also, perform any preprocessing, error handling, and post-processing.
   */
  protected void processAndPublishFileIfValid(Publisher publisher, FileReference fileReference, TraversalParams params) {
    URI fullPath = fileReference.getFullPath();
    String fileExtension = fileReference.getFileExtension();

    try {
      // Skip the file if it's not valid (a directory), params exclude it, or pre-processing fails.
      // (preprocessing is currently a NO-OP unless a subclass overrides it)
      if (!fileReference.isValidFile()
          || !params.includeFile(fileReference.getName(), fileReference.getLastModified())
          || !beforeProcessingFile(fullPath)) {
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
            handleArchiveFiles(publisher, compressorStream, fullPath, params);
          } else {
            String filePathFormat = fullPath + ARCHIVE_FILE_SEPARATOR + FilenameUtils.getName(decompressedPath);
            // if file is a supported file type that should be handled by a file handler
            if (params.supportedFileExtension(resolvedExtension)) {
              handleStreamExtensionFiles(publisher, resolvedExtension, params, compressorStream, filePathFormat);
            } else {
              Document doc = fileReference.decompressedFileAsDoc(compressorStream, filePathFormat, params);
              try (MDCCloseable mdc = MDC.putCloseable(Document.ID_FIELD, doc.getId())) {
                docLogger.info("StorageClient to publish Document {}.", doc.getId());
              }
              publisher.publish(doc);
            }
          }
        }
        afterProcessingFile(fullPath, params);
        return;
      }

      // handle archived files if needed to the end
      if (params.getHandleArchivedFiles() && isSupportedArchiveFileType(fullPath.toString())) {
        try (InputStream is = fileReference.getContentStream(params)) {
          handleArchiveFiles(publisher, is, fullPath, params);
        }
        afterProcessingFile(fullPath, params);
        return;
      }

      // handle file types using fileHandler if needed to the end
      if (params.supportedFileExtension(fileExtension)) {
        // Get a stream for the file content, so we don't have to load it all at once.
        InputStream contentStream = fileReference.getContentStream(params);
        // get the right FileHandler and publish based on content
        publishUsingFileHandler(publisher, fileExtension, params, contentStream, fullPath);

        afterProcessingFile(fullPath, params);
        return;
      }

      // handle normal files
      Document doc = fileReference.asDoc(params);
      try (MDCCloseable mdc = MDC.putCloseable(Document.ID_FIELD, doc.getId())) {
        docLogger.info("StorageClient to publish Document {}.", doc.getId());
      }
      publisher.publish(doc);
      afterProcessingFile(fullPath, params);
    } catch (UnsupportedOperationException e) {
      throw new UnsupportedOperationException("Encountered unsupported operation", e);
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
  private void handleArchiveFiles(Publisher publisher, InputStream inputStream, URI fullPathURI, TraversalParams params) throws ArchiveException, IOException, ConnectorException {
    try (BufferedInputStream bis = new BufferedInputStream(inputStream);
        ArchiveInputStream<? extends ArchiveEntry> in = new ArchiveStreamFactory().createArchiveInputStream(bis)) {
      ArchiveEntry entry = null;

      while ((entry = in.getNextEntry()) != null) {
        String entryFullPathStr = getArchiveEntryFullPath(fullPathURI, entry.getName());
        if (!in.canReadEntryData(entry)) {
          log.info("Cannot read entry data for entry: '{}' in '{}'. Skipping...", entry.getName(), fullPathURI);
          continue;
        }
        // checking validity only for the entries
        if (!entry.isDirectory() && params.includeFile(entry.getName(), entry.getLastModifiedDate().toInstant())) {
          String entryExtension = FilenameUtils.getExtension(entry.getName());
          if (params.supportedFileExtension(entryExtension)) {
            handleStreamExtensionFiles(publisher, entryExtension, params, in, entryFullPathStr);
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
              throw new ConnectorException("Error occurred while publishing archive entry: " + entry.getName() + " in " + fullPathURI, e);
            }
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
   *                    e.g. gs://bucket-name/folder/file.zip:entry.json OR path/to/example.csv
   * @throws ConnectorException If an error occurs processing / handling the file.
   */
  private void handleStreamExtensionFiles(Publisher publisher, String fileExtension, TraversalParams params, InputStream in, String fullPathStr)
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
      moveFile(filePath, params.getMoveToAfterProcessing());
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
      moveFile(filePath, params.getMoveToErrorFolder());
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
   * Should only be used for testing. Sets this storage client to be initialized.
   */
  void initializeForTesting() {
    this.initialized = true;
  }
}
