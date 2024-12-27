package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.FileConnector.CONTENT;
import static com.kmwllc.lucille.connector.FileConnector.FILE_PATH;
import static com.kmwllc.lucille.connector.FileConnector.GET_FILE_CONTENT;
import static com.kmwllc.lucille.connector.FileConnector.HANDLE_ARCHIVED_FILES;
import static com.kmwllc.lucille.connector.FileConnector.HANDLE_COMPRESSED_FILES;
import static com.kmwllc.lucille.connector.FileConnector.MODIFIED;
import static com.kmwllc.lucille.connector.FileConnector.MOVE_TO_AFTER_PROCESSING;
import static com.kmwllc.lucille.connector.FileConnector.MOVE_TO_ERROR_FOLDER;
import static com.kmwllc.lucille.connector.FileConnector.SIZE;
import static com.kmwllc.lucille.core.fileHandler.FileHandler.SUPPORTED_FILE_TYPES;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.fileHandler.FileHandler;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
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

  protected String docIdPrefix;
  protected URI pathToStorageURI;
  protected String bucketOrContainerName;
  protected String startingDirectory;
  protected List<Pattern> excludes;
  protected List<Pattern> includes;
  protected Map<String, Object> cloudOptions;
  protected Config fileOptions;
  protected Map<String, FileHandler> fileHandlers;
  protected Integer maxNumOfPages;
  protected boolean getFileContent;
  protected boolean handleArchivedFiles;
  protected boolean handleCompressedFiles;
  protected String moveToAfterProcessing;
  protected String moveToErrorFolder;

  public BaseStorageClient(URI pathToStorageURI, String docIdPrefix, List<Pattern> excludes, List<Pattern> includes,
      Map<String, Object> cloudOptions, Config fileOptions) {
    this.docIdPrefix = docIdPrefix;
    this.pathToStorageURI = pathToStorageURI;
    this.bucketOrContainerName = getContainerOrBucketName();
    this.startingDirectory = getStartingDirectory();
    this.excludes = excludes;
    this.includes = includes;
    this.cloudOptions = cloudOptions;
    this.fileOptions = fileOptions;
    this.fileHandlers = new HashMap<>();
    this.getFileContent = !fileOptions.hasPath(GET_FILE_CONTENT) || fileOptions.getBoolean(GET_FILE_CONTENT);
    this.handleArchivedFiles = fileOptions.hasPath(HANDLE_ARCHIVED_FILES) && fileOptions.getBoolean(HANDLE_ARCHIVED_FILES);
    this.handleCompressedFiles = fileOptions.hasPath(HANDLE_COMPRESSED_FILES) && fileOptions.getBoolean(HANDLE_COMPRESSED_FILES);
    this.moveToAfterProcessing = fileOptions.hasPath(MOVE_TO_AFTER_PROCESSING) ? fileOptions.getString(MOVE_TO_AFTER_PROCESSING) : null;
    this.moveToErrorFolder = fileOptions.hasPath(MOVE_TO_ERROR_FOLDER) ? fileOptions.getString(MOVE_TO_ERROR_FOLDER) : null;
    this.maxNumOfPages = cloudOptions.containsKey("maxNumOfPages") ? (Integer) cloudOptions.get("maxNumOfPages") : 100;
  }

  /**
   * Methods for traversing and processing files
   */
  protected void tryProcessAndPublishFile(Publisher publisher, String fullPathStr, String fileExtension, FileReference fileReference) {
    try {
      // preprocessing
      beforeProcessingFile(fullPathStr);

      // handle compressed files if needed to the end
      if (handleCompressedFiles && isSupportedCompressedFileType(fullPathStr)) {
        // unzip the file, compressorStream will be closed when try block is exited
        try (BufferedInputStream bis = new BufferedInputStream(getFileReferenceContentStream(fileReference));
            CompressorInputStream compressorStream = new CompressorStreamFactory().createCompressorInputStream(bis)) {
          // we can remove the last extension from path knowing before we confirmed that it has a compressed extension
          String decompressedFullPathStr = FilenameUtils.removeExtension(fullPathStr);
          if (handleArchivedFiles && isSupportedArchiveFileType(decompressedFullPathStr)) {
            handleArchiveFiles(publisher, compressorStream, fullPathStr);
          } else if (!fileOptions.isEmpty() && FileHandler.supportAndContainFileType(FilenameUtils.getExtension(decompressedFullPathStr), fileOptions)) {
            handleStreamExtensionFiles(publisher, compressorStream, FilenameUtils.getExtension(decompressedFullPathStr), decompressedFullPathStr);
          } else {
            Document doc = convertFileReferenceToDoc(fileReference, compressorStream, decompressedFullPathStr);
            publisher.publish(doc);
          }
        }
        afterProcessingFile(fullPathStr);
        return;
      }

      // handle archived files if needed to the end
      if (handleArchivedFiles && isSupportedArchiveFileType(fullPathStr)) {
        try (InputStream is = getFileReferenceContentStream(fileReference)) {
          handleArchiveFiles(publisher, is, fullPathStr);
        }
        afterProcessingFile(fullPathStr);
        return;
      }

      // handle file types using fileHandler if needed to the end
      if (!fileOptions.isEmpty() && FileHandler.supportAndContainFileType(fileExtension, fileOptions)) {

        if (fileReference.isCloudFileReference()) {
          // get the file content
          byte[] content = getFileReferenceContent(fileReference);
          // get the right FileHandler and publish based on content
          publishUsingFileHandler(publisher, fileExtension, content, fullPathStr);
        } else {
          // get path instead to be less resource intensive
          Path path = fileReference.getPath();
          // get the right FileHandler and publish based on content
          publishUsingFileHandler(publisher, fileExtension, path);
        }
        afterProcessingFile(fullPathStr);
        return;
      }

      // handle normal files
      Document doc = convertFileReferenceToDoc(fileReference);
      publisher.publish(doc);
      afterProcessingFile(fullPathStr);
    } catch (UnsupportedOperationException e) {
      throw new UnsupportedOperationException("Encountered unsupported operation", e);
    } catch (Exception e) {
      try {
        errorProcessingFile(fullPathStr);
      } catch (IOException ex) {
        log.error("Error occurred while performing error operations on file '{}'", fullPathStr, ex);
      }
      log.error("Unable to publish document '{}', SKIPPING", fullPathStr, e);
    }
  }

  // inputStream parameter will be closed via the try with resources
  protected void handleArchiveFiles(Publisher publisher, InputStream inputStream, String fullPathStr) throws ArchiveException, IOException, ConnectorException {
    try (BufferedInputStream bis = new BufferedInputStream(inputStream);
        ArchiveInputStream<? extends ArchiveEntry> in = new ArchiveStreamFactory().createArchiveInputStream(bis)) {
      ArchiveEntry entry = null;
      while ((entry = in.getNextEntry()) != null) {
        String entryFullPathStr = getEntryFullPath(fullPathStr, entry.getName());
        if (!in.canReadEntryData(entry)) {
          log.info("Cannot read entry data for entry: '{}' in '{}'. Skipping...", entry.getName(), fullPathStr);
          continue;
        }
        // checking validity only for the entries
        if (!entry.isDirectory() && shouldIncludeFile(entry.getName(), includes, excludes)) {
          String entryExtension = FilenameUtils.getExtension(entryFullPathStr);
          if (!fileOptions.isEmpty() && FileHandler.supportAndContainFileType(entryExtension, fileOptions)) {
            handleStreamExtensionFiles(publisher, in, entryExtension, entryFullPathStr);
          } else {
            // handle entry to be published as a normal document
            // note that if there exists a file within the same parent directory with the same name as the entries, it will have the same id
            Document doc = Document.create(createDocId(DigestUtils.md5Hex(entryFullPathStr)));
            doc.setField(FILE_PATH, entryFullPathStr);
            doc.setField(MODIFIED, entry.getLastModifiedDate().toInstant());
            // entry does not have creation date
            // note that some ArchiveEntry implementations may not have a size so will return -1
            doc.setField(SIZE, entry.getSize());
            if (getFileContent) {
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

  // cannot close inputStream as we are iterating through the archived stream and may have more files to process
  protected void handleStreamExtensionFiles(Publisher publisher, InputStream in, String fileExtension, String fullPathStr)
      throws ConnectorException {
    try {
      FileHandler handler = fileHandlers.get(fileExtension);
      handler.processFileAndPublish(publisher, in.readAllBytes(), fullPathStr);
    } catch (Exception e) {
      throw new ConnectorException("Error occurred while handling file: " + fullPathStr, e);
    }
  }

  protected void publishUsingFileHandler(Publisher publisher, String fileExtension, Path path) throws Exception {
    FileHandler handler = fileHandlers.get(fileExtension);
    if (handler == null) {
      throw new ConnectorException("No file handler found for file extension: " + fileExtension);
    }

    try {
      handler.processFileAndPublish(publisher, path);
    } catch (Exception e) {
      throw new ConnectorException("Error occurred while processing or publishing file: " + path, e);
    }
  }

  protected void publishUsingFileHandler(Publisher publisher, String fileExtension, byte[] content, String pathStr) throws Exception {
    FileHandler handler = fileHandlers.get(fileExtension);
    if (handler == null) {
      throw new ConnectorException("No file handler found for file extension: " + fileExtension);
    }

    try {
      handler.processFileAndPublish(publisher, content, pathStr);
    } catch (Exception e) {
      throw new ConnectorException("Error occurred while processing or publishing file: " + pathStr, e);
    }
  }

  protected String getEntryFullPath(String fullPathStr, String entryName) {
    // get parent directory
    int lastSlashIndex = fullPathStr.lastIndexOf("/");
    // handling different cases for zip and other archive files because entry names are different for different archive types
    if (fullPathStr.endsWith(".zip")) {
      return fullPathStr.substring(0, lastSlashIndex) + "/" + entryName;
    } else {
      String fileName = fullPathStr.substring(lastSlashIndex + 1);
      int firstExtensionIndex = fileName.indexOf(".");
      String resolvedFileName = firstExtensionIndex == -1 ? fileName : fileName.substring(0, firstExtensionIndex);
      return fullPathStr.substring(0, lastSlashIndex) + "/" + resolvedFileName + "/" + entryName;
    }
  }

  /**
   * Methods for performing operations before, after, during processing files
   */

  protected void beforeProcessingFile(String pathStr) throws Exception {
    createProcessedAndErrorFoldersIfSet(pathStr);
  }

  protected void afterProcessingFile(String pathStr) throws IOException {
    if (moveToAfterProcessing != null) {
      // move to processed folder
      moveFile(pathStr, moveToAfterProcessing);
    }
  }

  protected void errorProcessingFile(String pathStr) throws IOException {
    if (moveToErrorFolder != null) {
      // move to error folder
      moveFile(pathStr, moveToErrorFolder);
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

  private void createProcessedAndErrorFoldersIfSet(String pathStr) {
    Path path = Paths.get(pathStr);

    // if file does not exist locally, means it is a cloud path, not supported yet
    boolean sourceFileExistsLocally = Files.exists(path);
    if (moveToAfterProcessing != null && sourceFileExistsLocally) {
      // Create the destination directory if it doesn't exist.
      File destDir = new File(moveToAfterProcessing);
      if (!destDir.exists()) {
        destDir.mkdirs();
      }
    }
    if (moveToErrorFolder != null && sourceFileExistsLocally) {
      File errorDir = new File(moveToErrorFolder);
      if (!errorDir.exists()) {
        log.info("Creating error directory {}", errorDir.getAbsolutePath());
        errorDir.mkdirs();
      }
    }
  }

  /**
   * abstract methods
   */

  protected abstract Document convertFileReferenceToDoc(FileReference fileReference);

  protected abstract Document convertFileReferenceToDoc(FileReference fileReference, InputStream in, String fullPathStr);

  protected abstract byte[] getFileReferenceContent(FileReference fileReference);

  protected abstract InputStream getFileReferenceContentStream(FileReference fileReference);

  /**
   * helper methods
   */

  protected String getContainerOrBucketName() {
    return pathToStorageURI.getAuthority();
  }

  protected String getStartingDirectory() {
    String startingDirectory = Objects.equals(pathToStorageURI.getPath(), "/") ? "" : pathToStorageURI.getPath();
    if (startingDirectory.startsWith("/")) return startingDirectory.substring(1);
    return startingDirectory;
  }

  protected static boolean shouldIncludeFile(String pathStr, List<Pattern> includes, List<Pattern> excludes) {
    return excludes.stream().noneMatch(pattern -> pattern.matcher(pathStr).matches())
        && (includes.isEmpty() || includes.stream().anyMatch(pattern -> pattern.matcher(pathStr).matches()));
  }

  protected void initializeFileHandlers() throws ConnectorException {
    // go through fileOptions, and initialize all file handlers
    for (String fileExtensionSupported : SUPPORTED_FILE_TYPES) {
      if (fileOptions.hasPath(fileExtensionSupported)) {
        try {
          FileHandler handler = FileHandler.create(fileExtensionSupported, fileOptions);
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

  protected void clearFileHandlers() {
    fileHandlers.clear();
  }

  // note that the commented following are supported by apache-commons compress, but have yet to been tested, so commented out for now
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

  //TODO: sync with abstract connector
  protected String createDocId(String docId) {
    return docIdPrefix + docId;
  }
}
