package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.FileConnector.CONTENT;
import static com.kmwllc.lucille.connector.FileConnector.CREATED;
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
import java.nio.file.attribute.BasicFileAttributes;
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
      String cloudFileName = createFileNameFromCloudPath(pathStr);
      Path cloudFilePath = targetFolderPath.resolve(cloudFileName);
      Files.createFile(cloudFilePath);
      log.debug("Created placeholder file for cloud path at: {}", cloudFilePath);
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

  // InputStream parameter will be closed outside of this method as well
  public Document pathToDoc(Path path, InputStream in) throws ConnectorException {
    final String docId = DigestUtils.md5Hex(path.toString());
    final Document doc = Document.create(createDocId(docId));

    try {
      // get file attributes
      BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);

      // setting fields on document
      // remove Extension to show that we have decompressed the file and obtained its information
      doc.setField(FILE_PATH, FilenameUtils.removeExtension(path.toAbsolutePath().normalize().toString()));
      doc.setField(MODIFIED, attrs.lastModifiedTime().toInstant());
      doc.setField(CREATED, attrs.creationTime().toInstant());
      // TODO: find out how to get the size of the decompressed file
      if (getFileContent) doc.setField(CONTENT, in.readAllBytes());
    } catch (Exception e) {
      throw new ConnectorException("Error occurred getting/setting file attributes to document: " + path, e);
    }
    return doc;
  }

  public boolean isSupportedCompressedFileType(String string) {
    return string.endsWith(".gz");
    // string.endsWith(".bz2") ||
    // string.endsWith(".xz") ||
    // string.endsWith(".lzma") ||
    // string.endsWith(".br") ||
    // string.endsWith(".pack") ||
    // string.endsWith(".zst") ||
    // string.endsWith(".Z");
  }

  public boolean isSupportedCompressedFileType(Path path) {
    String fileName = path.getFileName().toString();

    return isSupportedCompressedFileType(fileName);
  }

  public boolean isSupportedArchiveFileType(Path path) {
    String fileName = path.getFileName().toString();
    return isSupportedArchiveFileType(fileName);
  }

  public boolean isSupportedArchiveFileType(String string) {
    // note that the following are supported by apache-commons compress, but have yet to been tested, so commented out for now
    return string.endsWith(".tar") ||
        string.endsWith(".zip");
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
