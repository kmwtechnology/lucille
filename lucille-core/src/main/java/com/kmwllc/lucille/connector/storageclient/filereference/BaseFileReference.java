package com.kmwllc.lucille.connector.storageclient.filereference;

import java.time.Instant;

public abstract class BaseFileReference implements FileReference {

  private final Instant lastModified;

  public BaseFileReference(Instant lastModified) {
    this.lastModified = lastModified;
  }

  /**
   * Returns the instant at which this file reference was last modified.
   */
  @Override
  public Instant getLastModified() {
    return this.lastModified;
  }
}
