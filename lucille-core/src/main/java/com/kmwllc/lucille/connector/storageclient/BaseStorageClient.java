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

  private static final Logger log = LoggerFactory.getLogger(StorageClient.class);

  protected String docIdPrefix;
  protected URI pathToStorageURI;
  protected String bucketOrContainerName;
  protected String startingDirectory;
  List<Pattern> excludes;
  List<Pattern> includes;
  Map<String, Object> cloudOptions;
  Config fileOptions;
  Map<String, FileHandler> fileHandlers;
  public Integer maxNumOfPages;
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
   * Methods below are methods used for traversing the client and publishing files
   */

  public void tryProcessAndPublishFile(Publisher publisher, String pathStr, String fileExtension, FileReference fileReference) {
    try {
      // preprocessing
      beforeProcessingFile(pathStr);

      // handle compressed files if needed
      if (handleCompressedFiles && isSupportedCompressedFileType(pathStr)) {
        // unzip the file, compressorStream will be closed when try block is exited
        try (BufferedInputStream bis = new BufferedInputStream(getFileReferenceContentStream(fileReference));
            CompressorInputStream compressorStream = new CompressorStreamFactory().createCompressorInputStream(bis)) {
          // we can remove the last extension from path knowing before we confirmed that it has a compressed extension
          String unzippedFileName = FilenameUtils.removeExtension(pathStr);
          if (handleArchivedFiles && isSupportedArchiveFileType(unzippedFileName)) {
            handleArchiveFiles(publisher, compressorStream);
          } else if (!fileOptions.isEmpty() && FileHandler.supportAndContainFileType(FilenameUtils.getExtension(unzippedFileName), fileOptions)) {
            handleStreamExtensionFiles(publisher, compressorStream, FilenameUtils.getExtension(unzippedFileName), pathStr);
          } else {
            Document doc = convertFileReferenceToDoc(fileReference, bucketOrContainerName, compressorStream, unzippedFileName);
            publisher.publish(doc);
          }
        }
        afterProcessingFile(pathStr);
        return;
      }

      // handle archived files if needed
      if (handleArchivedFiles && isSupportedArchiveFileType(pathStr)) {
        try (InputStream is = getFileReferenceContentStream(fileReference)) {
          handleArchiveFiles(publisher, is);
        }
        afterProcessingFile(pathStr);
        return;
      }

      // handle file types if needed
      if (!fileOptions.isEmpty() && FileHandler.supportAndContainFileType(fileExtension, fileOptions)) {

        if (fileReference.isCloudFileReference()) {
          // get the file content
          byte[] content = getFileReferenceContent(fileReference);
          // get the right FileHandler and publish based on content
          publishUsingFileHandler(publisher, fileExtension, content, pathStr);
        } else {
          // get path instead to be less resource intensive
          Path path = fileReference.getPath();
          // get the right FileHandler and publish based on content
          publishUsingFileHandler(publisher, fileExtension, path);
        }
        afterProcessingFile(pathStr);
        return;
      }

      Document doc = convertFileReferenceToDoc(fileReference, bucketOrContainerName);
      publisher.publish(doc);
      afterProcessingFile(pathStr);
    } catch (Exception e) {
      try {
        errorProcessingFile(pathStr);
      } catch (IOException ex) {
        log.error("Error occurred while performing error operations on file '{}'", pathStr, ex);
      }
      log.error("Unable to publish document '{}', SKIPPING", pathStr, e);
    }
  }

  public void publishUsingFileHandler(Publisher publisher, String fileExtension, Path path) throws Exception {
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

  public void publishUsingFileHandler(Publisher publisher, String fileExtension, byte[] content, String pathStr) throws Exception {
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

  // inputStream parameter will be closed via the try with resources
  public void handleArchiveFiles(Publisher publisher, InputStream inputStream) throws ArchiveException, IOException, ConnectorException {
    try (BufferedInputStream bis = new BufferedInputStream(inputStream);
        ArchiveInputStream<? extends ArchiveEntry> in = new ArchiveStreamFactory().createArchiveInputStream(bis)) {
      ArchiveEntry entry = null;
      while ((entry = in.getNextEntry()) != null) {
        if (!in.canReadEntryData(entry)) {
          log.info("Cannot read entry data for entry: '{}'. Skipping...", entry.getName());
          continue;
        }
        if (!entry.isDirectory() && shouldIncludeFile(entry.getName(), includes, excludes)) {
          String entryExtension = FilenameUtils.getExtension(entry.getName());
          if (!fileOptions.isEmpty() && FileHandler.supportAndContainFileType(entryExtension, fileOptions)) {
            handleStreamExtensionFiles(publisher, in, entryExtension, entry.getName());
          } else {
            // handle entry to be published as a normal document
            Document doc = Document.create(createDocId(DigestUtils.md5Hex(entry.getName())));
            doc.setField(FILE_PATH, entry.getName());
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
              throw new ConnectorException("Error occurred while publishing archive entry: " + entry.getName(), e);
            }
          }
        }
      }
    }
  }

  // cannot close inputStream as we are iterating through the archived stream and may have more files to process
  public void handleStreamExtensionFiles(Publisher publisher, InputStream in, String fileExtension, String fileName)
      throws ConnectorException {
    try {
      FileHandler handler = fileHandlers.get(fileExtension);
      handler.processFileAndPublish(publisher, in.readAllBytes(), fileName);
    } catch (Exception e) {
      throw new ConnectorException("Error occurred while handling file: " + fileName, e);
    }
  }

  /**
   * Methods below are methods used for performing operations before, after, during processing files
   */

  public void beforeProcessingFile(String pathStr) throws Exception {
    createProcessedAndErrorFoldersIfSet();
  }

  public void afterProcessingFile(String pathStr) throws IOException {
    if (moveToAfterProcessing != null) {
      // move to processed folder
      moveFile(pathStr, moveToAfterProcessing);
    }
  }

  public void errorProcessingFile(String pathStr) throws IOException {
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

    // ensure target folder exists as a precaution
    if (!Files.exists(targetFolderPath)) {
      Files.createDirectory(targetFolderPath);
    }

    // check if the file exists locally
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

  private String createFileNameFromCloudPath(String cloudPath) {
    // replace characters to make a valid filename
    return cloudPath.replaceAll("[^a-zA-Z0-9.-]", "_");
  }

  private void createProcessedAndErrorFoldersIfSet() {
    if (moveToAfterProcessing != null) {
      // Create the destination directory if it doesn't exist.
      File destDir = new File(moveToAfterProcessing);
      if (!destDir.exists()) {
        destDir.mkdirs();
      }
    }

    if (moveToErrorFolder != null) {
      File errorDir = new File(moveToErrorFolder);
      if (!errorDir.exists()) {
        log.info("Creating error directory {}", errorDir.getAbsolutePath());
        errorDir.mkdirs();
      }
    }
  }

  /**
   * Misc methods
   */

  public abstract Document convertFileReferenceToDoc(FileReference fileReference, String bucketOrContainerName);

  public abstract Document convertFileReferenceToDoc(FileReference fileReference, String bucketOrContainerName, InputStream in, String fileName);

  public abstract byte[] getFileReferenceContent(FileReference fileReference);

  public abstract InputStream getFileReferenceContentStream(FileReference fileReference);

  public String getContainerOrBucketName() {
    return pathToStorageURI.getAuthority();
  }

  public String getStartingDirectory() {
    String startingDirectory = Objects.equals(pathToStorageURI.getPath(), "/") ? "" : pathToStorageURI.getPath();
    if (startingDirectory.startsWith("/")) return startingDirectory.substring(1);
    return startingDirectory;
  }

  public static boolean shouldIncludeFile(String pathStr, List<Pattern> includes, List<Pattern> excludes) {
    return excludes.stream().noneMatch(pattern -> pattern.matcher(pathStr).matches())
        && (includes.isEmpty() || includes.stream().anyMatch(pattern -> pattern.matcher(pathStr).matches()));
  }

  public void initializeFileHandlers() throws ConnectorException {
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

  public void clearFileHandlers() {
    fileHandlers.clear();
  }

  // note that the commented following are supported by apache-commons compress, but have yet to been tested, so commented out for now
  public boolean isSupportedCompressedFileType(String pathStr) {
    return pathStr.endsWith(".gz");
    // string.endsWith(".bz2") ||
    // string.endsWith(".xz") ||
    // string.endsWith(".lzma") ||
    // string.endsWith(".br") ||
    // string.endsWith(".pack") ||
    // string.endsWith(".zst") ||
    // string.endsWith(".Z");
  }

  public boolean isSupportedArchiveFileType(String pathStr) {
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
  public String createDocId(String docId) {
    return docIdPrefix + docId;
  }
}
