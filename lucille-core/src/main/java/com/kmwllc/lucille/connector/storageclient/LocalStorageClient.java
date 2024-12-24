package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.FileConnector.CONTENT;
import static com.kmwllc.lucille.connector.FileConnector.CREATED;
import static com.kmwllc.lucille.connector.FileConnector.FILE_PATH;
import static com.kmwllc.lucille.connector.FileConnector.MODIFIED;
import static com.kmwllc.lucille.connector.FileConnector.SIZE;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalStorageClient extends BaseStorageClient {
  private static final Logger log = LoggerFactory.getLogger(LocalStorageClient.class);
  private FileSystem fs;
  private Path startingDirectoryPath;

  public LocalStorageClient(URI pathToStorageURI, String docIdPrefix, List<Pattern> excludes, List<Pattern> includes,
      Map<String, Object> cloudOptions, Config fileOptions) {
    super(pathToStorageURI, docIdPrefix, excludes, includes, cloudOptions, fileOptions);
  }

  @Override
  public void init() {
    fs = FileSystems.getDefault();
    // get current working directory path
    startingDirectoryPath = fs.getPath(startingDirectory);

    try {
      initializeFileHandlers();
    } catch (ConnectorException e) {
      throw new IllegalArgumentException("Error occurred initializing FileHandlers", e);
    }
  }

  @Override
  protected String getStartingDirectory() {
    return pathToStorageURI.getPath();
  }

  @Override
  public void shutdown() throws Exception {
    if (fs != null) {
      try {
        fs.close();
      } catch (UnsupportedOperationException e) {
        // Some file systems may not need closing
        fs = null;
      } catch (IOException e) {
        throw new IOException("Failed to close file system.", e);
      }
    }
    // clear all FileHandlers if any
    clearFileHandlers();
  }

  @Override
  public void traverse(Publisher publisher) throws Exception {
    try (Stream<Path> paths = Files.walk(startingDirectoryPath)) {
      paths.filter(this::isValidPath)
          .forEachOrdered(path -> {
            String pathStr = path.toString();
            String fileExtension = FilenameUtils.getExtension(path.toString());
            tryProcessAndPublishFile(publisher, pathStr, fileExtension, new FileReference(path));
          });
    } catch (InvalidPathException e) {
      throw new ConnectorException("Path string provided cannot be converted to a Path.", e);
    } catch (SecurityException | IOException e) {
      throw new ConnectorException("Error while traversing file system.", e);
    }
  }

  @Override
  protected Document convertFileReferenceToDoc(FileReference fileReference, String bucketOrContainerName) {
    Path path = fileReference.getPath();
    try {
      return pathToDoc(path);
    } catch (ConnectorException e) {
      throw new IllegalArgumentException("Unable to convert path '" + path + "' to Document", e);
    }
  }

  @Override
  protected Document convertFileReferenceToDoc(FileReference fileReference, String bucketOrContainerName, InputStream in, String fileName) {
    Path path = fileReference.getPath();
    try {
      return pathToDoc(path, in);
    } catch (ConnectorException e) {
      throw new IllegalArgumentException("Unable to convert path '" + path + "' to Document", e);
    }
  }

  @Override
  protected byte[] getFileReferenceContent(FileReference fileReference) {
    Path path = fileReference.getPath();
    try {
      return Files.readAllBytes(path);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to get content of path '" + path + "'", e);
    }
  }

  @Override
  protected InputStream getFileReferenceContentStream(FileReference fileReference) {
    Path path = fileReference.getPath();
    try {
      return Files.newInputStream(path);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to get content stream of path '" + path + "'", e);
    }
  }

  private boolean isValidPath(Path path) {
    if (!Files.isRegularFile(path)) {
      return false;
    }

    return shouldIncludeFile(path.toString(), includes, excludes);
  }

  private Document pathToDoc(Path path) throws ConnectorException {
    final String docId = DigestUtils.md5Hex(path.toString());
    final Document doc = Document.create(createDocId(docId));

    try {
      // get file attributes
      BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);

      // setting fields on document
      doc.setField(FILE_PATH, path.toAbsolutePath().normalize().toString());
      doc.setField(MODIFIED, attrs.lastModifiedTime().toInstant());
      doc.setField(CREATED, attrs.creationTime().toInstant());
      doc.setField(SIZE, attrs.size());
      if (getFileContent) doc.setField(CONTENT, Files.readAllBytes(path));
    } catch (Exception e) {
      throw new ConnectorException("Error occurred getting/setting file attributes to document: " + path, e);
    }
    return doc;
  }

  private Document pathToDoc(Path path, InputStream in) throws ConnectorException {
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
      // unable to get decompressed file size
      if (getFileContent) doc.setField(CONTENT, in.readAllBytes());
    } catch (Exception e) {
      throw new ConnectorException("Error occurred getting/setting file attributes to document: " + path, e);
    }
    return doc;
  }
}
