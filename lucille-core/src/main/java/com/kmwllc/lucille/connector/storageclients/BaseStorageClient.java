package com.kmwllc.lucille.connector.storageclients;

import static com.kmwllc.lucille.connector.FileConnector.CONTENT;
import static com.kmwllc.lucille.connector.FileConnector.CREATED;
import static com.kmwllc.lucille.connector.FileConnector.FILE_PATH;
import static com.kmwllc.lucille.connector.FileConnector.GET_FILE_CONTENT;
import static com.kmwllc.lucille.connector.FileConnector.HANDLE_ARCHIVED_FILES;
import static com.kmwllc.lucille.connector.FileConnector.HANDLE_COMPRESSED_FILES;
import static com.kmwllc.lucille.connector.FileConnector.MODIFIED;
import static com.kmwllc.lucille.connector.FileConnector.SIZE;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.fileHandlers.FileHandler;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Iterator;
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

public abstract class BaseStorageClient implements StorageClient {

  protected Publisher publisher;
  protected String docIdPrefix;
  protected URI pathToStorageURI;
  protected String bucketOrContainerName;
  protected String startingDirectory;
  List<Pattern> excludes;
  List<Pattern> includes;
  Map<String, Object> cloudOptions;
  Config fileOptions;
  public Integer maxNumOfPages;
  protected boolean getFileContent;
  protected boolean handleArchivedFiles;
  protected boolean handleCompressedFiles;

  public BaseStorageClient(URI pathToStorageURI, Publisher publisher, String docIdPrefix, List<Pattern> excludes, List<Pattern> includes,
      Map<String, Object> cloudOptions, Config fileOptions) {
    this.publisher = publisher;
    this.docIdPrefix = docIdPrefix;
    this.pathToStorageURI = pathToStorageURI;
    this.bucketOrContainerName = getContainerOrBucketName();
    this.startingDirectory = getStartingDirectory();
    this.excludes = excludes;
    this.includes = includes;
    this.cloudOptions = cloudOptions;
    this.fileOptions = fileOptions;
    this.getFileContent = !fileOptions.hasPath(GET_FILE_CONTENT) || fileOptions.getBoolean(GET_FILE_CONTENT);
    this.handleArchivedFiles = fileOptions.hasPath(HANDLE_ARCHIVED_FILES) && fileOptions.getBoolean(HANDLE_ARCHIVED_FILES);
    this.handleCompressedFiles = fileOptions.hasPath(HANDLE_COMPRESSED_FILES) && fileOptions.getBoolean(HANDLE_COMPRESSED_FILES);
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

  public static boolean shouldIncludeFile(String filePath, List<Pattern> includes, List<Pattern> excludes) {
    return excludes.stream().noneMatch(pattern -> pattern.matcher(filePath).matches())
        && (includes.isEmpty() || includes.stream().anyMatch(pattern -> pattern.matcher(filePath).matches()));
  }

  public void publishUsingFileHandler(String fileExtension, Path path) throws Exception {
    FileHandler handler = null;
    try {
      handler = FileHandler.getFileHandler(fileExtension, fileOptions);
    } catch (Exception e) {
      // throwing exception will skip this file
      throw new ConnectorException("Error occurred while getting file handler for file: " + path, e);
    }
    // perform preprocessing
    handler.beforeProcessingFile(path);
    Iterator<Document> docIterator;
    try {
      docIterator = handler.processFile(path);
    } catch (Exception e) {
      // going to skip this file if an error occurs
      log.error("Unable to set up iterator for this file '{}', SKIPPING", path, e);
      handler.errorProcessingFile(path);
      return;
    }
    // once docIterator.hasNext() is false, it will close its resources in handler and return
    while (docIterator.hasNext()) {
      try {
        Document doc = docIterator.next();
        if (doc != null) {
          publisher.publish(doc);
        }
      } catch (Exception e) {
        // if we fail to publish a document, we log the error and continue to the next document
        // to "finish" the iterator and close its resources
        log.error("Unable to publish document '{}', SKIPPING", path, e);
        handler.errorProcessingFile(path);
      }
    }
    // all iterations have been successfully published, perform afterProcessing
    handler.afterProcessingFile(path);
  }

  public void publishUsingFileHandler(String fileExtension, byte[] content, String pathStr) throws Exception {
    FileHandler handler = null;
    try {
      handler = FileHandler.getFileHandler(fileExtension, fileOptions);
    } catch (Exception e) {
      // throwing exception will skip this file
      throw new ConnectorException("Error occurred while getting file handler for file: " + pathStr, e);
    }
    // perform preprocessing
    handler.beforeProcessingFile(content);
    Iterator<Document> docIterator;
    try {
      docIterator = handler.processFile(content, pathStr);
    } catch (Exception e) {
      // going to skip this file if an error occurs
      log.error("Unable to set up iterator for this file '{}', SKIPPING", pathStr, e);
      handler.errorProcessingFile(content);
      return;
    }
    // once docIterator.hasNext() is false, it will close its resources in handler and return
    while (docIterator.hasNext()) {
      try {
        Document doc = docIterator.next();
        if (doc != null) {
          publisher.publish(doc);
        }
      } catch (Exception e) {
        // if we fail to publish a document, we log the error and continue to the next document
        // to "finish" the iterator and close its resources
        log.error("Unable to publish document '{}', SKIPPING", pathStr, e);
        handler.errorProcessingFile(content);
      }
    }
    // all iterations have been successfully published, perform afterProcessing
    handler.afterProcessingFile(content);
  }

  // inputStream parameter will be closed outside of this method as well
  private void handleArchiveFiles(Publisher publisher, InputStream inputStream) throws ArchiveException, IOException, ConnectorException {
    try (BufferedInputStream bis = new BufferedInputStream(inputStream);
        ArchiveInputStream<? extends ArchiveEntry> in = new ArchiveStreamFactory().createArchiveInputStream(bis)) {
      ArchiveEntry entry = null;
      while ((entry = in.getNextEntry()) != null) {
        if (shouldIncludeFile(entry.getName(), includes, excludes) && !entry.isDirectory()) {
          String entryExtension = FilenameUtils.getExtension(entry.getName());
          if (FileHandler.supportsFileType(entryExtension, fileOptions)) {
            handleStreamExtensionFiles(publisher, in, entryExtension, entry.getName());
          } else {
            // handle entry to be published as a normal document
            Document doc = Document.create(createDocId(DigestUtils.md5Hex(entry.getName())));
            doc.setField(FILE_PATH, entry.getName());
            doc.setField(MODIFIED, entry.getLastModifiedDate().toInstant());
            // entry does not have creation date
            // some cases where entry.getSize() returns -1, so we use in.readAllBytes instead...
            // cannot use in.available as it shows the remaining bytes including the rest of the files in the archive
            byte[] content = in.readAllBytes();
            doc.setField(SIZE, content.length);
            if (getFileContent) {
              doc.setField(CONTENT, content);
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

  // inputStream parameter will be closed outside of this method as well
  private void handleStreamExtensionFiles(Publisher publisher, InputStream in, String fileExtension, String fileName)
      throws ConnectorException {
    try {
      FileHandler handler = FileHandler.getFileHandler(fileExtension, fileOptions);
      Iterator<Document> docIterator = handler.processFile(in.readAllBytes(), fileName);
      while (docIterator.hasNext()) {
        publisher.publish(docIterator.next());
      }
    } catch (Exception e) {
      throw new ConnectorException("Error occurred while handling file: " + fileName, e);
    }
  }

  // InputStream parameter will be closed outside of this method as well
  private Document pathToDoc(Path path, InputStream in) throws ConnectorException {
    final String docId = DigestUtils.md5Hex(path.toString());
    final Document doc = Document.create(createDocId(docId));

    try {
      // get file attributes
      BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);

      // setting fields on document
      doc.setField(FILE_PATH, path.toAbsolutePath().normalize().toString());
      doc.setField(MODIFIED, attrs.lastModifiedTime().toInstant());
      doc.setField(CREATED, attrs.creationTime().toInstant());
      // wont be able to get the size unless we read from the stream :/ so have to readBytes even though we set getFileContent to false
      byte[] content = in.readAllBytes();
      doc.setField(SIZE, content.length);
      if (getFileContent) doc.setField(CONTENT, content);
    } catch (Exception e) {
      throw new ConnectorException("Error occurred getting/setting file attributes to document: " + path, e);
    }
    return doc;
  }

  private boolean isSupportedCompressedFileType(Path path) {
    String fileName = path.getFileName().toString();

    // note that the following are supported by apache-commons-compress, but have yet to been tested, so commented out for now
    return fileName.endsWith(".gz");
    // fileName.endsWith(".bz2") ||
    // fileName.endsWith(".xz") ||
    // fileName.endsWith(".lzma") ||
    // fileName.endsWith(".br") ||
    // fileName.endsWith(".pack") ||
    // fileName.endsWith(".zst") ||
    // fileName.endsWith(".Z");
  }

  private boolean isSupportedArchiveFileType(Path path) {
    String fileName = path.getFileName().toString();
    return isSupportedArchiveFileType(fileName);
  }

  private boolean isSupportedArchiveFileType(String string) {
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
