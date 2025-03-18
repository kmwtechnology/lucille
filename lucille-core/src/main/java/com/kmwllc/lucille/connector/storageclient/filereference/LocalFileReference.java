package com.kmwllc.lucille.connector.storageclient.filereference;

import com.kmwllc.lucille.connector.storageclient.TraversalParams;
import java.nio.file.Path;
import java.time.Instant;

/**
 * A file reference to a file on the local file system.
 */
public class LocalFileReference extends BaseFileReference {

  private final Path path;

  // The provided path should be absolute and normalized.
  public LocalFileReference(Path path, Instant lastModified) {
    super(lastModified);

    this.path = path;
  }

  @Override
  public String getFilePath(TraversalParams params) {
    return path.toString();
  }

  @Override
  public boolean isCloudFileReference() {
    return false;
  }

  @Override
  public boolean validFile() {
    return true;
  }

  // Returning the actual path object
  public Path getPath() {
    return path;
  }
}
