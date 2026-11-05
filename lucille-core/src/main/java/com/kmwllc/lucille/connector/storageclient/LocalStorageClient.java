package com.kmwllc.lucille.connector.storageclient;

import com.kmwllc.lucille.core.Publisher;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A storage client for the local file system. Needs no configuration.
 */
public class LocalStorageClient extends BaseStorageClient {

  private static final Logger log = LoggerFactory.getLogger(LocalStorageClient.class);

  public LocalStorageClient() {
    super(ConfigFactory.empty());
  }

  @Override
  protected void initializeStorageClient() throws IOException { }

  @Override
  protected void shutdownStorageClient() throws IOException { }

  @Override
  protected void traverseStorageClient(Publisher publisher, TraversalParams params, FileStateTracker stateMgr) throws IOException {
    Path root = toPath(params.getURI());

    // Fail rather than finish a traversal that saw nothing: with state, every file previously published under a missing
    // or unreadable root (e.g. an unmounted share) would otherwise be tombstoned and deleted from state.
    if (!Files.exists(root)) {
      throw new NoSuchFileException(root.toString(), null, "The path to traverse does not exist.");
    }

    if (!Files.isReadable(root)) {
      throw new AccessDeniedException(root.toString(), null, "The path to traverse is not readable.");
    }

    Files.walkFileTree(root, new LocalFileVisitor(publisher, params, stateMgr));
  }

  @Override
  protected InputStream getFileContentStreamFromStorage(URI uri) throws IOException {
    File file = new File(uri);

    return new FileInputStream(file);
  }

  private static Path toPath(URI uri) {
    return uri.isAbsolute() ? Paths.get(uri) : Paths.get(uri.getPath());
  }

  /**
   * {@inheritDoc}
   * <p> Resolves both paths to an absolute, normalized form so a relative path and an absolute path to the same
   * directory are recognized as the same. Does not resolve symlinks.
   */
  @Override
  public boolean containsPath(URI parent, URI child) {
    Path parentPath = toPath(parent).toAbsolutePath().normalize();
    Path childPath = toPath(child).toAbsolutePath().normalize();

    return childPath.startsWith(parentPath);
  }

  @Override
  public void moveFile(URI filePath, URI folder) throws IOException {
    Path pathForFile = toPath(filePath);
    Path pathForFolder = toPath(folder);

    // ensure the target folder and any missing parents exist
    Files.createDirectories(pathForFolder);

    // move the local file to the target folder
    Path targetPath = pathForFolder.resolve(pathForFile.getFileName());
    Files.move(pathForFile, targetPath, StandardCopyOption.REPLACE_EXISTING);
    log.debug("Moved local file to: {}", targetPath);
  }

  public class LocalFileVisitor implements FileVisitor<Path> {

    private Publisher publisher;
    private TraversalParams params;
    private final FileStateTracker stateMgr;
    private final Path root;

    public LocalFileVisitor(Publisher publisher, TraversalParams params, FileStateTracker stateMgr) {
      this.publisher = publisher;
      this.params = params;
      this.stateMgr = stateMgr;
      this.root = toPath(params.getURI()).toAbsolutePath().normalize();
    }

    @Override
    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
      URI dirURI = dir.toAbsolutePath().normalize().toUri();

      if (isSkippedDirectory(dirURI, params)) {
        return FileVisitResult.SKIP_SUBTREE;
      } else {
        return FileVisitResult.CONTINUE;
      }
    }

    private static String ensureTrailingSlash(String s) {
      return s.endsWith("/") ? s : s + "/";
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
      // Visit the file and actually process it!
      FileReference fileRef = new LocalFileReference(file, attrs);
      processAndPublishFileIfValid(publisher, fileRef, params, stateMgr);
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
      Path failedPath = file.toAbsolutePath().normalize();

      // nothing under the root can be traversed, so stop rather than tombstone everything under it
      if (failedPath.equals(root)) {
        throw exc;
      }

      log.warn("Couldn't read '{}' during traversal. It is skipped, and is kept in state if state is in use.", failedPath, exc);

      // The path still exists, so neither it nor (if it is a directory) anything under it may be treated as deleted.
      if (stateMgr != null) {
        String pathStr = failedPath.toUri().toString();

        // a directory has no row of its own. If its type can't be read, protect it both ways.
        if (!Files.isDirectory(failedPath)) {
          stateMgr.markFileEncountered(pathStr);
        }

        stateMgr.markAllEntriesEncountered(ensureTrailingSlash(pathStr));
      }

      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
      // The listing ended early (e.g. a mount dropped), so the files not yet listed would look deleted. Fail the run
      // rather than tombstone them.
      if (exc != null) {
        throw exc;
      }

      return FileVisitResult.CONTINUE;
    }
  }


  private class LocalFileReference extends BaseFileReference {

    private final Path path;

    // The given path will become absolute and will be normalized.
    public LocalFileReference(Path path, BasicFileAttributes attributes) {
      super(path.toAbsolutePath().normalize().toUri(), attributes.lastModifiedTime().toInstant(), attributes.size(), attributes.creationTime().toInstant());

      // Holding onto the path object since it makes some of the needed operations a bit more straightforward
      this.path = path.toAbsolutePath().normalize();
    }

    @Override
    public String getName() { return path.getFileName().toString(); }

    @Override
    public boolean isValidFile() {
      return true;
    }

    @Override
    public InputStream getContentStream(TraversalParams params) throws IOException {
      return Files.newInputStream(path);
    }

    @Override
    protected byte[] getFileContent(TraversalParams params) throws IOException {
      return Files.readAllBytes(path);
    }
  }
}
