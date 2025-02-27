package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.FileConnector.CONTENT;
import static com.kmwllc.lucille.connector.FileConnector.FILE_PATH;
import static com.kmwllc.lucille.connector.FileConnector.MAX_NUM_OF_PAGES;
import static com.kmwllc.lucille.connector.FileConnector.MODIFIED;
import static com.kmwllc.lucille.connector.FileConnector.SIZE;
import static com.kmwllc.lucille.connector.FileConnector.ARCHIVE_FILE_SEPARATOR;
import static com.kmwllc.lucille.core.fileHandler.FileHandler.SUPPORTED_FILE_TYPES;

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

public abstract class BaseStorageClient implements StorageClient {

  private static final Logger log = LoggerFactory.getLogger(BaseStorageClient.class);

  protected final Config config;
  protected final Map<String, FileHandler> fileHandlers;
  protected final int maxNumOfPages;

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

  @Override
  public final void traverse(Publisher publisher, TraversalParams params) throws Exception {
    if (!isInitialized()) {
      throw new IllegalStateException("This StorageClient has not been initialized.");
    }

    try {
      initializeFileHandlers(params);
      traverseStorageClient(publisher, params);
    } finally {
      clearFileHandlers();
    }
  }

  protected abstract void traverseStorageClient(Publisher publisher, TraversalParams params) throws Exception;

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
   * @param fullPathStr full path of the file, can be a cloud path or local path. Cloud path would include schema and bucket/container name
   *                    e.g. gs://bucket-name/folder/file.txt or s3://bucket-name/file.txt
   * @param fileExtension fileExtension of the file. Used to determine if the file should be processed by file handler
   * @param fileReference fileReference object that contains the Path for local Storage or Storage Item implementation for cloud storage
   */
  protected void tryProcessAndPublishFile(Publisher publisher, String fullPathStr, String fileExtension, FileReference fileReference, TraversalParams params) {
    try {
      // preprocessing, currently a NO-OP unless a subclass overrides it
      if (!beforeProcessingFile(fullPathStr)) {
        // The preprocessing check failed, let's skip the file.
        return;
      }
      
      // handle compressed files if needed to the end
      if (params.getHandleCompressedFiles() && isSupportedCompressedFileType(fullPathStr)) {
        // unzip the file, compressorStream will be closed when try block is exited
        try (BufferedInputStream bis = new BufferedInputStream(getFileReferenceContentStream(fileReference, params));
            CompressorInputStream compressorStream = new CompressorStreamFactory().createCompressorInputStream(bis)) {
          // we can remove the last extension from path knowing before we confirmed that it has a compressed extension
          String decompressedPath = FilenameUtils.removeExtension(fullPathStr);
          String resolvedExtension = FilenameUtils.getExtension(decompressedPath);

          // if detected to be an archive type after decompression
          if (params.getHandleArchivedFiles() && isSupportedArchiveFileType(decompressedPath)) {
            handleArchiveFiles(publisher, compressorStream, fullPathStr, params);
          } else {
            String filePathFormat = fullPathStr + ARCHIVE_FILE_SEPARATOR + FilenameUtils.getName(decompressedPath);
            // if file is a supported file type that should be handled by a file handler
            if (params.supportedFileType(resolvedExtension)) {
              handleStreamExtensionFiles(publisher, compressorStream, resolvedExtension, filePathFormat);
            } else {
              Document doc = convertFileReferenceToDoc(fileReference, compressorStream, filePathFormat, params);
              publisher.publish(doc);
            }
          }
        }
        afterProcessingFile(fullPathStr, params);
        return;
      }

      // handle archived files if needed to the end
      if (params.getHandleArchivedFiles() && isSupportedArchiveFileType(fullPathStr)) {
        try (InputStream is = getFileReferenceContentStream(fileReference, params)) {
          handleArchiveFiles(publisher, is, fullPathStr, params);
        }
        afterProcessingFile(fullPathStr, params);
        return;
      }

      // handle file types using fileHandler if needed to the end
      if (params.supportedFileType(fileExtension)) {
        // Get a stream for the file content, so we don't have to load it all at once.
        InputStream contentStream = getFileReferenceContentStream(fileReference);
        // get the right FileHandler and publish based on content
        publishUsingFileHandler(publisher, fileExtension, contentStream, fullPathStr);

        afterProcessingFile(fullPathStr);
        return;
      }

      // handle normal files
      Document doc = convertFileReferenceToDoc(fileReference, params);
      publisher.publish(doc);
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
  protected void handleArchiveFiles(Publisher publisher, InputStream inputStream, String fullPathStr, TraversalParams params) throws ArchiveException, IOException, ConnectorException {
    try (BufferedInputStream bis = new BufferedInputStream(inputStream);
        ArchiveInputStream<? extends ArchiveEntry> in = new ArchiveStreamFactory().createArchiveInputStream(bis)) {
      ArchiveEntry entry = null;

      while ((entry = in.getNextEntry()) != null) {
        String entryFullPathStr = getArchiveEntryFullPath(fullPathStr, entry.getName());
        if (!in.canReadEntryData(entry)) {
          log.info("Cannot read entry data for entry: '{}' in '{}'. Skipping...", entry.getName(), fullPathStr);
          continue;
        }
        // checking validity only for the entries
        if (!entry.isDirectory() && params.shouldIncludeFile(entry.getName())) {
          String entryExtension = FilenameUtils.getExtension(entry.getName());
          if (params.supportedFileType(entryExtension)) {
            handleStreamExtensionFiles(publisher, in, entryExtension, entryFullPathStr);
          } else {
            // handle entry to be published as a normal document
            // note that if there exists a file within the same parent directory with the same name as the entries, it will have the same id
            Document doc = Document.create(createDocId(DigestUtils.md5Hex(entryFullPathStr), params));
            doc.setField(FILE_PATH, entryFullPathStr);
            doc.setField(MODIFIED, entry.getLastModifiedDate().toInstant());
            // entry does not have creation date
            // note that some ArchiveEntry implementations may not have a size so will return -1
            doc.setField(SIZE, entry.getSize());
            if (params.shouldGetFileContent()) {
              doc.setField(CONTENT, in.readAllBytes());
            }
            try {
              publisher.publish(doc);
            } catch (Exception e) {
              throw new ConnectorException("Error occurred while publishing archive entry: " + entry.getName() + " in " + fullPathStr, e);
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
   * @param fullPathStr can be entry full path or decompressed full path. Can be a cloud path or local path.
   *                    e.g. gs://bucket-name/folder/file.zip:entry.json OR path/to/example.csv
   */
  protected void handleStreamExtensionFiles(Publisher publisher, InputStream in, String fileExtension, String fullPathStr)
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
   * helper method to publish using file handler using file content (InputStream)
   */
  protected void publishUsingFileHandler(Publisher publisher, String fileExtension, InputStream inputStream, String pathStr) throws Exception {
    FileHandler handler = fileHandlers.get(fileExtension);
    if (handler == null) {
      throw new ConnectorException("No file handler found for file extension: " + fileExtension);
    }

    try {
      handler.processFileAndPublish(publisher, inputStream, pathStr);
    } catch (Exception e) {
      throw new ConnectorException("Error occurred while processing or publishing file: " + pathStr, e);
    }
  }

  /**
   * helper method to get the full path of an entry in an archived file. Only used for archive files
   */
  protected String getArchiveEntryFullPath(String fullPathStr, String entryName) {
    return fullPathStr + ARCHIVE_FILE_SEPARATOR + entryName;
  }

  /**
   * method for performing operations before processing files. This method is ill be called before processing 
   * each file in traversal.  If the method returns true, the file will be processed.  A return of false indicates
   * the file should be skipped.
   */
  protected boolean beforeProcessingFile(String pathStr) throws Exception {
    // Base implementation, process all files. 
    return true;
  }

  /**
   * method for performing operations after processing files. Additional operations can be added
   * in the implementation of this method. Will be called after processing each file in traversal.
   */
  protected void afterProcessingFile(String pathStr, TraversalParams params) throws IOException {
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
  protected void errorProcessingFile(String pathStr, TraversalParams params) throws IOException {
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
   * converts a file reference (Path or cloud Storage object implementation) to a document.
   */
  protected abstract Document convertFileReferenceToDoc(FileReference fileReference, TraversalParams params);

  /**
   * will only be called in the scenario where after decompression and file will not be handled by a file handler
   */
  protected abstract Document convertFileReferenceToDoc(FileReference fileReference, InputStream in, String decompressedFullPathStr, TraversalParams params);

  /**
   * get the content of the file reference as an InputStream. Always called within a try-with-resources block
   */
  protected abstract InputStream getFileReferenceContentStream(FileReference fileReference, TraversalParams params);


  /**
   * Return the starting directory from the params. For Local, S3, and Google, it is the URI's path or an empty string,
   * with the first "/" removed if present. For Azure, it is everything after the third "/" in the URI. (Local, S3, and Google
   * defer to the given params, Azure does its own calculations.)
   */
  protected String getStartingDirectory(TraversalParams params) {
    return params.getStartingDirectory();
  }

  /**
   * Return the bucket/container name from the params. For Local, S3, and Google, this is the URI's authority.
   * For Azure, this is the content between the second and third "/". (Local, S3, and Google
   * defer to the given params, Azure does its own calculations.)
   */
  protected String getBucketOrContainerName(TraversalParams params) {
    return params.getBucketOrContainerName();
  }


  /**
   * helper method to initialize all file handlers based on the fileOptions
   */
  protected void initializeFileHandlers(TraversalParams params) throws ConnectorException {
    // go through fileOptions, and initialize all file handlers
    for (String fileExtensionSupported : SUPPORTED_FILE_TYPES) {
      if (params.optionsIncludeFileExtension(fileExtensionSupported)) {
        try {
          FileHandler handler = FileHandler.create(fileExtensionSupported, params.getFileOptions());
          fileHandlers.put(fileExtensionSupported, handler);
          // handle cases like json/jsonl
          if (fileExtensionSupported.equals("json") || fileExtensionSupported.equals("jsonl")) {
            fileHandlers.put("json", handler);
            fileHandlers.put("jsonl", handler);
          }
        } catch (Exception e) {
          throw new ConnectorException("Error occurred while putting in file handler for file extension: " + fileExtensionSupported, e);
        }
      }
    }
  }

  /**
   * clear all file handlers if any. Should be called in the shutdown method
   */
  protected void clearFileHandlers() {
    if (fileHandlers != null) {
      fileHandlers.clear();
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

  //should sync with abstract connector class?
  protected String createDocId(String docId, TraversalParams params) {
    return params.getDocIdPrefix() + docId;
  }

  // Only for testing
  void initializeForTesting() {
    this.initialized = true;
  }
}
