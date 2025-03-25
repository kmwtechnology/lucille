package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.FileConnector.CONTENT;
import static com.kmwllc.lucille.connector.FileConnector.CREATED;
import static com.kmwllc.lucille.connector.FileConnector.FILE_PATH;
import static com.kmwllc.lucille.connector.FileConnector.MODIFIED;
import static com.kmwllc.lucille.connector.FileConnector.SIZE;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalStorageClient extends BaseStorageClient {

  private static final Logger log = LoggerFactory.getLogger(LocalStorageClient.class);

  public LocalStorageClient() {
    super(ConfigFactory.empty());
  }

  // Config options do not matter for LocalStorageClient
  @Override
  protected void validateOptions(Config config) { }

  @Override
  protected void initializeStorageClient() throws IOException { }

  @Override
  protected void shutdownStorageClient() throws IOException { }

  @Override
  protected void traverseStorageClient(Publisher publisher, TraversalParams params) throws Exception {
    Files.walkFileTree(Paths.get(getStartingDirectory(params)), new LocalFileVisitor(publisher, params));
  }

  @Override
  protected InputStream getFileContentStreamFromStorage(URI uri) throws IOException {
    File file = new File(uri);

    return new FileInputStream(file);
  }

  private String getStartingDirectory(TraversalParams params) { return params.getURI().getPath(); }

  public class LocalFileVisitor implements FileVisitor<Path> {

    private Publisher publisher;
    private TraversalParams params;

    public LocalFileVisitor(Publisher publisher, TraversalParams params) {
      this.publisher = publisher;
      this.params = params;
    }

    @Override
    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
      // We don't care about preVisiting a directory
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
      // Visit the file and actually process it!
      FileReference fileRef = new LocalFileReference(file, attrs.lastModifiedTime().toInstant());
      processAndPublishFileIfValid(publisher, fileRef, params);
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
      // At some point we can add a feature to create a tombstone document, for now just log the failure.
      log.warn("Visit File Failed for : {}", file.toString(), exc);
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
      // we don't care about after we finished the directory
      return FileVisitResult.CONTINUE;
    }
  }


  private class LocalFileReference extends BaseFileReference {

    private final Path path;

    // The provided path should be absolute and normalized.
    public LocalFileReference(Path path, Instant lastModified) {
      super(lastModified);

      this.path = path;
    }

    @Override
    public String getName() { return path.getFileName().toString();}

    @Override
    public String getFullPath(TraversalParams params) {
      return path.toString();
    }

    @Override
    public boolean isValidFile() {
      return true;
    }

    @Override
    public InputStream getContentStream(TraversalParams params) {
      try {
        return Files.newInputStream(path);
      } catch (IOException e) {
        throw new IllegalArgumentException("Unable to get content stream of path '" + path + "'", e);
      }
    }

    @Override
    public Document asDoc(TraversalParams params) {
      String fullPath = path.toAbsolutePath().normalize().toString();
      String docId = DigestUtils.md5Hex(fullPath);
      Document doc = Document.create(createDocId(docId, params));

      try {
        BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);

        doc.setField(FILE_PATH, fullPath);
        doc.setField(SIZE, attrs.size());

        if (attrs.lastModifiedTime() != null) {
          doc.setField(MODIFIED, attrs.lastModifiedTime().toInstant());
        }

        if (attrs.creationTime() != null) {
          doc.setField(CREATED, attrs.creationTime().toInstant());
        }

        if (params.shouldGetFileContent()) {
          doc.setField(CONTENT, Files.readAllBytes(path));
        }
      } catch (IOException e) {
        throw new IllegalArgumentException("Unable to convert path '" + path + "' to Document", e);
      }
      return doc;
    }

    @Override
    public Document asDoc(InputStream in, String decompressedFullPathStr, TraversalParams params) throws IOException {
      String docId = DigestUtils.md5Hex(decompressedFullPathStr);
      Document doc = Document.create(createDocId(docId, params));

      // get file attributes
      BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);

      // setting fields on document
      // remove Extension to show that we have decompressed the file and obtained its information
      doc.setField(FILE_PATH, decompressedFullPathStr);
      doc.setField(MODIFIED, attrs.lastModifiedTime().toInstant());
      doc.setField(CREATED, attrs.creationTime().toInstant());
      // unable to get decompressed file size
      if (params.shouldGetFileContent()) {
        doc.setField(CONTENT, in.readAllBytes());
      }

      return doc;
    }
  }
}
