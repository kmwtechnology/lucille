package com.kmwllc.lucille.connector.storageclient;

import java.time.Instant;
import org.apache.commons.io.FilenameUtils;

public abstract class BaseFileReference implements FileReference {

  private final Instant lastModified;

  public BaseFileReference(Instant lastModified) {
    this.lastModified = lastModified;
  }

  @Override
  public Instant getLastModified() {
    return this.lastModified;
  }

  @Override
  public String getFileExtension() {
    String fileName = getName();
    return FilenameUtils.getExtension(fileName);
  }
}
